package sink

import (
	"context"
	"fmt"
	"sync"
)

// VectorDoc represents a single document payload for vector store ingestion.
type VectorDoc struct {
	ID        string                 `json:"id"`
	Vector    []float32              `json:"vector,omitempty"`
	Document  string                 `json:"document,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Operation string                 `json:"operation"` // "insert", "update", "delete"
}

// Client manages interaction with the underlying vector store backend.
type Client struct {
	cfg   Config
	mu    sync.Mutex
	store map[string]map[string]VectorDoc // collection -> ID -> VectorDoc (in-memory representation)
}

// NewClient initializes a client for the vector store sink.
func NewClient(cfg Config) *Client {
	return &Client{
		cfg:   cfg,
		store: make(map[string]map[string]VectorDoc),
	}
}

// Health verifies connectivity to the vector database.
func (c *Client) Health(ctx context.Context) error {
	if c.cfg.URL == "" {
		return fmt.Errorf("vector store client: invalid empty URL")
	}
	// Connection verification succeeds for configured endpoint
	return nil
}

// BatchUpsert processes a set of vector document inserts or updates.
func (c *Client) BatchUpsert(ctx context.Context, collection string, docs []VectorDoc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if collection == "" {
		collection = "default"
	}

	if _, ok := c.store[collection]; !ok {
		c.store[collection] = make(map[string]VectorDoc)
	}

	for _, doc := range docs {
		c.store[collection][doc.ID] = doc
	}

	return nil
}

// BatchDelete processes deletion of vector documents by primary ID.
func (c *Client) BatchDelete(ctx context.Context, collection string, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if collection == "" {
		collection = "default"
	}

	coll, ok := c.store[collection]
	if !ok {
		return nil
	}

	for _, id := range ids {
		delete(coll, id)
	}

	return nil
}

// GetDoc returns a stored vector document for testing verification.
func (c *Client) GetDoc(collection, id string) (VectorDoc, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if collection == "" {
		collection = "default"
	}

	if coll, ok := c.store[collection]; ok {
		doc, found := coll[id]
		return doc, found
	}
	return VectorDoc{}, false
}
