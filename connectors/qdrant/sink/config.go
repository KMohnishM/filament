// Package sink implements the filament.Sink interface for Qdrant vector store destinations.
package sink

import (
	"fmt"
	"strings"

	"github.com/galaxy-io/filament"
)

// defaultBatchSize specifies the default row limit per Qdrant point batch upsert.
const defaultBatchSize = 500

// Config defines the connection and execution settings for the Qdrant sink.
type Config struct {
	// URL specifies the Qdrant cluster REST endpoint URI (e.g. http://localhost:6333).
	URL string `json:"url"`

	// APIKey specifies the authentication API key or token for Qdrant Cloud.
	APIKey string `json:"api_key,omitempty"`

	// Collection specifies the target Qdrant collection name.
	Collection string `json:"collection,omitempty"`

	// PrimaryKey specifies the record column name used as unique document ID.
	PrimaryKey string `json:"primary_key,omitempty"`

	// TextFields lists column names used to construct document text content.
	TextFields []string `json:"text_fields,omitempty"`

	// EmbeddingField specifies an optional column containing pre-computed float vectors.
	EmbeddingField string `json:"embedding_field,omitempty"`

	// BatchSize specifies the maximum points written per batch.
	BatchSize int `json:"batch_size,omitempty"`
}

// ConfigSchema describes the configuration schema for UI catalog display and validation.
func ConfigSchema() filament.ConfigSchema {
	return filament.ConfigSchema{
		Properties: map[string]filament.PropertySpec{
			"url": {
				Type:        "string",
				Title:       "Qdrant Endpoint",
				Description: "Connection URI for Qdrant REST API (e.g., http://localhost:6333 or https://xyz.qdrant.tech).",
			},
			"api_key": {
				Type:        "string",
				Title:       "API Key",
				Description: "Qdrant API Key or cluster authorization token.",
				Secret:      true,
			},
			"collection": {
				Type:        "string",
				Title:       "Collection Name",
				Description: "Target Qdrant collection name.",
			},
			"primary_key": {
				Type:        "string",
				Title:       "Primary Key Field",
				Description: "Column name in source records mapped as point ID.",
			},
			"text_fields": {
				Type:        "array",
				Title:       "Text Payload Fields",
				Description: "Column names to concatenate for document text.",
			},
			"embedding_field": {
				Type:        "string",
				Title:       "Pre-computed Embedding Field",
				Description: "Column name containing pre-computed float32 array vectors.",
			},
			"batch_size": {
				Type:        "integer",
				Title:       "Batch Size",
				Description: "Maximum points per batch write.",
				Default:     defaultBatchSize,
			},
		},
		Required: []string{"url"},
	}
}

// ParseConfig decodes and validates a filament.Config object into a Config struct.
func ParseConfig(cfg filament.Config) (Config, error) {
	c := Config{
		BatchSize: defaultBatchSize,
	}

	c.URL = cfg.String("url")
	if strings.TrimSpace(c.URL) == "" {
		return c, fmt.Errorf("qdrant sink: 'url' is required")
	}

	if val := cfg.String("api_key"); val != "" {
		c.APIKey = val
	}

	if val := cfg.String("collection"); val != "" {
		c.Collection = val
	}

	if val := cfg.String("primary_key"); val != "" {
		c.PrimaryKey = val
	}

	if raw, ok := cfg.Raw()["text_fields"]; ok {
		switch v := raw.(type) {
		case []string:
			c.TextFields = v
		case []any:
			var fields []string
			for _, item := range v {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					fields = append(fields, strings.TrimSpace(s))
				}
			}
			c.TextFields = fields
		case string:
			parts := strings.Split(v, ",")
			var fields []string
			for _, p := range parts {
				if s := strings.TrimSpace(p); s != "" {
					fields = append(fields, s)
				}
			}
			c.TextFields = fields
		}
	}

	if val := cfg.String("embedding_field"); val != "" {
		c.EmbeddingField = val
	}

	if size := cfg.Int("batch_size"); size > 0 {
		c.BatchSize = size
	}

	return c, nil
}
