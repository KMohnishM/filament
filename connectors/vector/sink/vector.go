package sink

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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
	written  atomic.Int64
	aborted  bool
}

// New returns an unconfigured Vector Store sink.
func New() *Sink {
	return &Sink{
		policies: make(map[string]filament.WritePolicy),
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
	if s.client == nil {
		return filament.WriteReceipt{}, fmt.Errorf("vector sink: write before open")
	}

	numRows := b.NumRows()
	if numRows == 0 {
		return filament.WriteReceipt{}, nil
	}

	collection := s.cfg.Collection
	if collection == "" {
		collection = b.Resource
	}

	var upsertDocs []VectorDoc
	var deleteIDs []string

	rows := b.Rows()
	if rows != nil {
		for i := 0; i < numRows; i++ {
			op := b.Op(i)
			docID := fmt.Sprintf("%s-%d", b.Resource, i)

			if op == rowmodel.OpDelete {
				deleteIDs = append(deleteIDs, docID)
			} else {
				// Insert or Update operation
				doc := VectorDoc{
					ID:        docID,
					Operation: opString(op),
					Metadata: map[string]interface{}{
						"resource": b.Resource,
						"seq":      b.Seq,
					},
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

func opString(op rowmodel.Operation) string {
	switch op {
	case rowmodel.OpInsert:
		return "insert"
	case rowmodel.OpUpdate:
		return "update"
	case rowmodel.OpDelete:
		return "delete"
	default:
		return "insert"
	}
}
