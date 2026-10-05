package sink

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/galaxy-io/filament"
	"github.com/galaxy-io/filament/arrowbatch"
	"github.com/galaxy-io/filament/rowmodel"
)

// Sink writes Arrow record batches into Vector Store destinations.
type Sink struct {
	mu       sync.Mutex
	cfg      Config
	client   *Client
	run      filament.RunID
	policies map[string]filament.WritePolicy
	cleared  map[string]struct{}
	written  atomic.Int64
	aborted  bool
}

// New returns an unconfigured Vector Store sink.
func New() *Sink {
	return &Sink{
		policies: make(map[string]filament.WritePolicy),
		cleared:  make(map[string]struct{}),
	}
}

var (
	_ filament.Sink              = (*Sink)(nil)
	_ filament.ConfigValidatable = (*Sink)(nil)
	_ filament.LiveValidatable   = (*Sink)(nil)
	_ filament.Schematized       = (*Sink)(nil)
)

// Spec describes the sink's capabilities, write policies, and configuration schema.
func (s *Sink) Spec() filament.SinkSpec {
	return filament.SinkSpec{
		Name:         "vector",
		DisplayName:  "Vector Store",
		Description:  "High-performance CDC sync for vector databases (PGVector, ChromaDB, Qdrant, Milvus).",
		DarkLogoURL:  "https://cdn.getgalaxy.io/sources/source-icon-vector-dark.svg",
		LightLogoURL: "https://cdn.getgalaxy.io/sources/source-icon-vector-light.svg",
		Version:      "1",
		Config:       ConfigSchema(),
		Capabilities: filament.SinkCapabilities{
			Schematized:        true,
			PreferredBatchRows: defaultBatchSize,
			WritePolicies: filament.WriteCapabilities(
				filament.IngestionFullReplace,
				filament.IngestionFullAppend,
				filament.IngestionFullUpsert,
				filament.IngestionIncrementalUpsert,
				filament.IngestionCDCMerge,
			),
		},
		SchemaField: "collection",
	}
}

// Name identifies this sink implementation.
func (s *Sink) Name() string { return "vector" }

// Validate checks connection configuration syntax without making network requests.
func (s *Sink) Validate(cfg filament.Config) error {
	_, err := ParseConfig(cfg)
	return err
}

// TestConnection verifies connectivity to the vector database.
func (s *Sink) TestConnection(ctx context.Context, cfg filament.Config) error {
	parsed, err := ParseConfig(cfg)
	if err != nil {
		return err
	}
	client := NewClient(parsed)
	return client.Health(ctx)
}

// Open prepares the sink for an ingestion run.
func (s *Sink) Open(ctx context.Context, run filament.RunSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parsed, err := ParseConfig(filament.NewConfig(run.Sink.Config))
	if err != nil {
		return fmt.Errorf("vector sink: open: %w", err)
	}

	s.cfg = parsed
	s.client = NewClient(parsed)
	s.run = run.Run
	s.policies = run.WritePolicies
	s.cleared = make(map[string]struct{})
	s.aborted = false

	return nil
}

// EnsureSchema prepares collection settings before records arrive.
func (s *Sink) EnsureSchema(ctx context.Context, resource string, schema rowmodel.Schema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client == nil {
		return fmt.Errorf("vector sink: ensure schema before open")
	}

	return nil
}

// Apply converts an Arrow batch into vector operations (upsert / delete) and writes to the store.
func (s *Sink) Apply(ctx context.Context, b *arrowbatch.Batch, opts filament.ApplyOptions) (filament.WriteReceipt, error) {
	s.mu.Lock()
	if s.client == nil {
		s.mu.Unlock()
		return filament.WriteReceipt{}, fmt.Errorf("vector sink: write before open")
	}

	if s.aborted {
		s.mu.Unlock()
		return filament.WriteReceipt{}, fmt.Errorf("vector sink: cannot apply batch to aborted run")
	}

	collection := s.cfg.Collection
	if collection == "" {
		collection = b.Resource
	}

	// Handle IngestionFullReplace by purging target collection on first batch of run
	mode := opts.Policy.Capability.Mode
	if mode == filament.WriteReplace {
		if _, done := s.cleared[collection]; !done {
			if err := s.client.DeleteAllDocuments(ctx, collection); err != nil {
				s.mu.Unlock()
				return filament.WriteReceipt{}, fmt.Errorf("vector sink: clear collection %q for replace: %w", collection, err)
			}
			s.cleared[collection] = struct{}{}
		}
	}
	s.mu.Unlock()

	numRows := b.NumRows()
	if numRows == 0 {
		return filament.WriteReceipt{}, nil
	}

	// Step 1: Validate ALL operations before processing any writes
	opStrings := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		op := b.Op(i)
		opStr, err := opString(op)
		if err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("vector sink: row %d has invalid operation: %w", i, err)
		}
		opStrings[i] = opStr
	}

	rows := b.Rows()
	var upsertDocs []VectorDoc
	var deleteIDs []string

	if rows != nil {
		schema := rows.Schema()
		pkColIdx := findColumnIndex(schema, s.cfg.PrimaryKey)

		for i := 0; i < numRows; i++ {
			docID := extractDocID(rows, i, pkColIdx, b.Resource)
			opStr := opStrings[i]

			if opStr == "delete" {
				deleteIDs = append(deleteIDs, docID)
			} else {
				// Insert or Update operation: extract Document text, Vector, and Metadata from Arrow row
				docText := extractDocumentText(rows, i, s.cfg.TextFields)
				vector := extractEmbeddingVector(rows, i, s.cfg.EmbeddingField)
				meta := extractMetadataMap(rows, i, s.cfg.EmbeddingField, b.Resource, b.Seq)

				doc := VectorDoc{
					ID:        docID,
					Vector:    vector,
					Document:  docText,
					Operation: opStr,
					Metadata:  meta,
				}
				upsertDocs = append(upsertDocs, doc)
			}
		}
	}

	if len(upsertDocs) > 0 {
		if err := s.client.BatchUpsert(ctx, collection, upsertDocs); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("vector sink: batch upsert: %w", err)
		}
	}

	if len(deleteIDs) > 0 {
		if err := s.client.BatchDelete(ctx, collection, deleteIDs); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("vector sink: batch delete: %w", err)
		}
	}

	s.written.Add(int64(numRows))
	return filament.WriteReceipt{RowsWritten: int64(numRows)}, nil
}

// Commit finalizes active batch transactions.
func (s *Sink) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return fmt.Errorf("vector sink: cannot commit aborted run")
	}

	return nil
}

// Abort rolls back pending uncommitted operations.
func (s *Sink) Abort(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aborted = true
	return nil
}

// opString converts a rowmodel.Operation enum to its string representation ("insert", "update", "delete").
// Returns an error if the operation kind is unknown.
func opString(op rowmodel.Operation) (string, error) {
	switch op {
	case rowmodel.OpInsert:
		return "insert", nil
	case rowmodel.OpUpdate:
		return "update", nil
	case rowmodel.OpDelete:
		return "delete", nil
	default:
		return "", fmt.Errorf("unsupported row operation kind: %d", op)
	}
}

// findColumnIndex locates a column index by name, with fallback search for "id", "_filament_key", or "_id".
func findColumnIndex(schema *arrow.Schema, targetCol string) int {
	if schema == nil {
		return -1
	}

	if targetCol != "" {
		for i, field := range schema.Fields() {
			if strings.EqualFold(field.Name, targetCol) {
				return i
			}
		}
	}

	// Fallback field candidates
	candidates := []string{"id", "_filament_key", "_id", "uuid"}
	for _, c := range candidates {
		for i, field := range schema.Fields() {
			if strings.EqualFold(field.Name, c) {
				return i
			}
		}
	}

	return 0
}

// extractDocID extracts the primary key string value from the specified column for row i.
func extractDocID(rows arrow.RecordBatch, rowIdx int, colIdx int, resource string) string {
	if rows == nil || colIdx < 0 || colIdx >= int(rows.NumCols()) {
		return fmt.Sprintf("%s-%d", resource, rowIdx)
	}

	col := rows.Column(colIdx)
	if col == nil || col.IsNull(rowIdx) {
		return fmt.Sprintf("%s-%d", resource, rowIdx)
	}

	valStr := col.ValueStr(rowIdx)
	if strings.TrimSpace(valStr) == "" {
		return fmt.Sprintf("%s-%d", resource, rowIdx)
	}

	return valStr
}

// extractDocumentText concatenates values from specified text fields for row i.
func extractDocumentText(rows arrow.RecordBatch, rowIdx int, textFields []string) string {
	if rows == nil {
		return ""
	}

	schema := rows.Schema()
	var parts []string

	if len(textFields) > 0 {
		for _, fieldName := range textFields {
			for idx, f := range schema.Fields() {
				if strings.EqualFold(f.Name, fieldName) {
					col := rows.Column(idx)
					if col != nil && !col.IsNull(rowIdx) {
						parts = append(parts, col.ValueStr(rowIdx))
					}
					break
				}
			}
		}
	} else {
		// Fallback: concatenate all string columns
		for idx, f := range schema.Fields() {
			if f.Type.ID() == arrow.STRING {
				col := rows.Column(idx)
				if col != nil && !col.IsNull(rowIdx) {
					parts = append(parts, col.ValueStr(rowIdx))
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// extractEmbeddingVector extracts a []float32 array from the embedding column if present.
func extractEmbeddingVector(rows arrow.RecordBatch, rowIdx int, embeddingCol string) []float32 {
	if rows == nil || embeddingCol == "" {
		return nil
	}

	schema := rows.Schema()
	for idx, f := range schema.Fields() {
		if strings.EqualFold(f.Name, embeddingCol) {
			col := rows.Column(idx)
			if col == nil || col.IsNull(rowIdx) {
				return nil
			}
			// Attempt string parsing of array format "[0.1, 0.2, ...]" or raw float array
			valStr := col.ValueStr(rowIdx)
			return parseFloats(valStr)
		}
	}

	return nil
}

// extractMetadataMap extracts non-vector column values into a key-value metadata map.
func extractMetadataMap(rows arrow.RecordBatch, rowIdx int, embeddingCol string, resource string, seq uint64) map[string]interface{} {
	meta := map[string]interface{}{
		"resource": resource,
		"seq":      seq,
	}

	if rows == nil {
		return meta
	}

	schema := rows.Schema()
	for idx, f := range schema.Fields() {
		if strings.EqualFold(f.Name, embeddingCol) {
			continue
		}
		col := rows.Column(idx)
		if col != nil && !col.IsNull(rowIdx) {
			meta[f.Name] = col.ValueStr(rowIdx)
		}
	}

	return meta
}

// parseFloats converts a string formatted float list (e.g. "[0.1, 0.2]") into a []float32 slice.
func parseFloats(s string) []float32 {
	clean := strings.Trim(s, "[]{} ")
	if clean == "" {
		return nil
	}

	tokens := strings.Split(clean, ",")
	res := make([]float32, 0, len(tokens))

	for _, tok := range tokens {
		val, err := strconv.ParseFloat(strings.TrimSpace(tok), 32)
		if err == nil {
			res = append(res, float32(val))
		}
	}

	return res
}
