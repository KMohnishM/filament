// Package sink implements the filament.Sink interface for Vector Store destinations.
package sink

import (
	"fmt"
	"strings"

	"github.com/galaxy-io/filament"
)

// defaultBatchSize specifies the default row limit per vector batch upsert.
const defaultBatchSize = 500

// Config defines the connection and execution settings for the Vector Store sink.
type Config struct {
	// URL specifies the host endpoint URI for the target vector database.
	URL string `json:"url"`

	// APIKey specifies the authentication secret or token for the vector store.
	APIKey string `json:"api_key,omitempty"`

	// Provider identifies the vector store backend (pgvector, chroma, qdrant, milvus, mock).
	Provider string `json:"provider"`

	// Collection specifies the target collection or table for document storage.
	Collection string `json:"collection,omitempty"`

	// PrimaryKey specifies the field name used as unique vector document ID.
	PrimaryKey string `json:"primary_key,omitempty"`

	// TextFields lists record column names used to build the text payload for embedding.
	TextFields []string `json:"text_fields,omitempty"`

	// EmbeddingField specifies an optional field containing pre-computed vector embeddings.
	EmbeddingField string `json:"embedding_field,omitempty"`

	// EmbeddingProvider identifies the embedding service (openai, ollama, huggingface, custom_http, precomputed).
	EmbeddingProvider string `json:"embedding_provider,omitempty"`

	// EmbeddingAPIKey specifies the secret API key for third-party embedding providers.
	EmbeddingAPIKey string `json:"embedding_api_key,omitempty"`

	// EmbeddingModel identifies the model used for embedding generation.
	EmbeddingModel string `json:"embedding_model,omitempty"`

	// BatchSize specifies the maximum row batch size per write operation.
	BatchSize int `json:"batch_size,omitempty"`
}

// ConfigSchema describes the configuration schema for UI catalog display and validation.
func ConfigSchema() filament.ConfigSchema {
	return filament.ConfigSchema{
		Properties: map[string]filament.PropertySpec{
			"url": {
				Type:        "string",
				Title:       "Vector Store Endpoint",
				Description: "Connection URI or host URL for the target vector database (e.g., http://localhost:6333 or postgresql://user:pass@localhost:5432/db).",
			},
			"api_key": {
				Type:        "string",
				Title:       "API Key",
				Description: "Authentication API Key or token for the vector store.",
				Secret:      true,
			},
			"provider": {
				Type:        "string",
				Title:       "Vector DB Provider",
				Description: "Target vector store engine (pgvector, chroma, qdrant, milvus, mock). Default is 'pgvector'.",
				Default:     "pgvector",
			},
			"collection": {
				Type:        "string",
				Title:       "Collection / Table",
				Description: "Target collection or table name for vector document storage.",
			},
			"primary_key": {
				Type:        "string",
				Title:       "Primary Key Field",
				Description: "Field name in source records to map as unique vector document ID.",
			},
			"text_fields": {
				Type:        "array",
				Title:       "Text Payload Fields",
				Description: "List of text field names to concatenate for embedding generation.",
			},
			"embedding_field": {
				Type:        "string",
				Title:       "Pre-computed Embedding Field",
				Description: "Optional field containing existing vector embeddings ([]float32 array).",
			},
			"embedding_provider": {
				Type:        "string",
				Title:       "Embedding Provider",
				Description: "Service for inline embedding generation (openai, ollama, huggingface, custom_http, precomputed). Default is 'precomputed'.",
				Default:     "precomputed",
			},
			"embedding_api_key": {
				Type:        "string",
				Title:       "Embedding Service API Key",
				Description: "API Key for third-party embedding models (e.g., OpenAI API Key).",
				Secret:      true,
			},
			"embedding_model": {
				Type:        "string",
				Title:       "Embedding Model",
				Description: "Embedding model identifier (e.g., text-embedding-3-small or all-minilm-l6-v2).",
			},
			"batch_size": {
				Type:        "integer",
				Title:       "Batch Size",
				Description: "Maximum number of rows per vector batch upsert.",
				Default:     defaultBatchSize,
			},
		},
		Required: []string{"url"},
	}
}

// ParseConfig decodes and validates a filament.Config object into a Config struct.
func ParseConfig(cfg filament.Config) (Config, error) {
	c := Config{
		Provider:          "pgvector",
		EmbeddingProvider: "precomputed",
		BatchSize:         defaultBatchSize,
	}

	c.URL = cfg.String("url")
	if strings.TrimSpace(c.URL) == "" {
		return c, fmt.Errorf("vector sink: 'url' is required")
	}

	if val := cfg.String("api_key"); val != "" {
		c.APIKey = val
	}

	if val := cfg.String("provider"); val != "" {
		c.Provider = strings.ToLower(val)
	}

	if val := cfg.String("collection"); val != "" {
		c.Collection = val
	}

	if val := cfg.String("primary_key"); val != "" {
		c.PrimaryKey = val
	}

	if val := cfg.String("embedding_field"); val != "" {
		c.EmbeddingField = val
	}

	if val := cfg.String("embedding_provider"); val != "" {
		c.EmbeddingProvider = strings.ToLower(val)
	}

	if val := cfg.String("embedding_api_key"); val != "" {
		c.EmbeddingAPIKey = val
	}

	if val := cfg.String("embedding_model"); val != "" {
		c.EmbeddingModel = val
	}

	if size := cfg.Int("batch_size"); size > 0 {
		c.BatchSize = size
	}

	return c, nil
}
