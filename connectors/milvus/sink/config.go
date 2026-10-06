// Package sink implements the filament.Sink interface for Milvus vector database destinations.
package sink

import (
	"fmt"
	"strings"

	"github.com/galaxy-io/filament"
)

// defaultBatchSize specifies the default row limit per Milvus insert batch.
const defaultBatchSize = 500

// Config defines the connection and execution settings for the Milvus sink.
type Config struct {
	// URL specifies the Milvus REST API / gRPC endpoint URI (e.g. https://xyz.zillizcloud.com).
	URL string `json:"url"`

	// APIKey specifies the authentication API key or token for Zilliz / Milvus Cloud.
	APIKey string `json:"api_key,omitempty"`

	// Collection specifies the target Milvus collection name.
	Collection string `json:"collection,omitempty"`

	// PrimaryKey specifies the column name used as primary key field.
	PrimaryKey string `json:"primary_key,omitempty"`

	// TextFields lists column names used to construct document text content.
	TextFields []string `json:"text_fields,omitempty"`

	// EmbeddingField specifies an optional column containing pre-computed float vectors.
	EmbeddingField string `json:"embedding_field,omitempty"`

	// BatchSize specifies the maximum entities written per batch.
	BatchSize int `json:"batch_size,omitempty"`
}

// ConfigSchema describes the configuration schema for UI catalog display and validation.
func ConfigSchema() filament.ConfigSchema {
	return filament.ConfigSchema{
		Properties: map[string]filament.PropertySpec{
			"url": {
				Type:        "string",
				Title:       "Milvus Endpoint",
				Description: "Connection URI for Milvus REST API (e.g., https://xyz.zillizcloud.com or http://localhost:19530).",
			},
			"api_key": {
				Type:        "string",
				Title:       "API Key / Token",
				Description: "Milvus / Zilliz Cloud authorization token.",
				Secret:      true,
			},
			"collection": {
				Type:        "string",
				Title:       "Collection Name",
				Description: "Target Milvus collection name.",
			},
			"primary_key": {
				Type:        "string",
				Title:       "Primary Key Field",
				Description: "Column name in source records mapped as entity primary key.",
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
				Description: "Maximum entities per batch write.",
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
		return c, fmt.Errorf("milvus sink: 'url' is required")
	}

	if val := cfg.String("api_key"); val != "" {
		c.APIKey = val
	}

	// Security check: require HTTPS when api_key is configured on remote endpoints
	if c.APIKey != "" && !strings.HasPrefix(c.URL, "https://") && !strings.HasPrefix(c.URL, "mock://") {
		return c, fmt.Errorf("milvus sink: HTTPS scheme (https://) is required when 'api_key' is configured")
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
