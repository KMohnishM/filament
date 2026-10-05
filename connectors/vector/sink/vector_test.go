package sink

import (
	"context"
	"testing"

	"github.com/galaxy-io/filament"
	"github.com/galaxy-io/filament/arrowbatch"
)

// TestVectorSink_Spec verifies that the Vector Store sink returns a non-empty name and display title.
func TestVectorSink_Spec(t *testing.T) {
	s := New()
	spec := s.Spec()

	if spec.Name != "vector" {
		t.Errorf("expected spec.Name to be 'vector', got %q", spec.Name)
	}

	if spec.DisplayName != "Vector Store" {
		t.Errorf("expected DisplayName to be 'Vector Store', got %q", spec.DisplayName)
	}
}

// TestVectorSink_ParseConfig verifies parsing of raw configuration options into a Config struct.
func TestVectorSink_ParseConfig(t *testing.T) {
	rawCfg := map[string]any{
		"url":                "http://localhost:6333",
		"provider":           "qdrant",
		"collection":         "documents",
		"text_fields":        []string{"title", "description"},
		"batch_size":         250,
		"embedding_provider": "openai",
	}

	cfg, err := ParseConfig(filament.NewConfig(rawCfg))
	if err != nil {
		t.Fatalf("unexpected error parsing config: %v", err)
	}

	if cfg.URL != "http://localhost:6333" {
		t.Errorf("expected URL to be 'http://localhost:6333', got %q", cfg.URL)
	}

	if cfg.Provider != "qdrant" {
		t.Errorf("expected Provider to be 'qdrant', got %q", cfg.Provider)
	}

	if len(cfg.TextFields) != 2 || cfg.TextFields[0] != "title" || cfg.TextFields[1] != "description" {
		t.Errorf("expected TextFields to be ['title', 'description'], got %v", cfg.TextFields)
	}

	if cfg.BatchSize != 250 {
		t.Errorf("expected BatchSize to be 250, got %d", cfg.BatchSize)
	}
}

// TestVectorSink_Lifecycle verifies Open, Apply, and Commit lifecycle execution for the vector sink.
func TestVectorSink_Lifecycle(t *testing.T) {
	ctx := context.Background()
	s := New()

	runSpec := filament.RunSpec{
		Run: filament.RunID("test-run-1"),
		Sink: filament.SinkConfig{
			Config: map[string]any{
				"url":        "mock://localhost:8000",
				"provider":   "mock",
				"collection": "test_collection",
			},
		},
	}

	if err := s.Open(ctx, runSpec); err != nil {
		t.Fatalf("failed to open vector sink: %v", err)
	}

	// Create marker batch
	batch := arrowbatch.NewMarker()
	batch.Resource = "users"

	receipt, err := s.Apply(ctx, batch, filament.ApplyOptions{})
	if err != nil {
		t.Fatalf("failed to apply batch: %v", err)
	}

	if receipt.RowsWritten != 0 {
		t.Errorf("expected 0 rows written for marker batch, got %d", receipt.RowsWritten)
	}

	if err := s.Commit(ctx); err != nil {
		t.Fatalf("failed to commit run: %v", err)
	}
}

// TestVectorSink_AbortRejection verifies that Apply rejects writes after Abort is called.
func TestVectorSink_AbortRejection(t *testing.T) {
	ctx := context.Background()
	s := New()

	runSpec := filament.RunSpec{
		Run: filament.RunID("test-run-abort"),
		Sink: filament.SinkConfig{
			Config: map[string]any{
				"url":      "mock://localhost:8000",
				"provider": "mock",
			},
		},
	}

	if err := s.Open(ctx, runSpec); err != nil {
		t.Fatalf("failed to open vector sink: %v", err)
	}

	if err := s.Abort(ctx); err != nil {
		t.Fatalf("failed to abort vector sink: %v", err)
	}

	batch := arrowbatch.NewMarker()
	batch.Resource = "users"

	_, err := s.Apply(ctx, batch, filament.ApplyOptions{})
	if err == nil {
		t.Fatalf("expected Apply to return an error after Abort, got nil")
	}
}

// TestParseFloats verifies valid float parsing and error reporting on invalid float tokens.
func TestParseFloats(t *testing.T) {
	floats, err := parseFloats("[0.1, 0.25, 0.5]")
	if err != nil {
		t.Fatalf("unexpected error parsing valid floats: %v", err)
	}
	if len(floats) != 3 {
		t.Fatalf("expected 3 floats, got %d", len(floats))
	}
	if floats[0] != 0.1 || floats[1] != 0.25 || floats[2] != 0.5 {
		t.Errorf("unexpected float values: %v", floats)
	}

	_, err = parseFloats("[0.1, invalid_token, 0.5]")
	if err == nil {
		t.Errorf("expected parseFloats to return an error on invalid token, got nil")
	}
}
