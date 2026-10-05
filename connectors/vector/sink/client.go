package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// VectorDoc represents a single document payload for vector store ingestion.
type VectorDoc struct {
	// ID is the unique primary key of the vector document.
	ID string `json:"id"`

	// Vector contains the floating point array representation of the document embedding.
	Vector []float32 `json:"vector,omitempty"`

	// Document contains the original textual content of the payload.
	Document string `json:"document,omitempty"`

	// Metadata holds arbitrary key-value attributes associated with the document.
	Metadata map[string]interface{} `json:"metadata,omitempty"`

	// Operation specifies the change operation kind ("insert", "update", or "delete").
	Operation string `json:"operation"`
}

// Client manages interaction with the target vector store backend.
type Client struct {
	cfg        Config
	httpClient *http.Client
	mu         sync.Mutex
	store      map[string]map[string]VectorDoc // in-memory fallback store for mock provider
}

// NewClient initializes a client instance for the vector store sink.
func NewClient(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		store: make(map[string]map[string]VectorDoc),
	}
}

// Health verifies connectivity to the configured vector database or mock provider.
func (c *Client) Health(ctx context.Context) error {
	if strings.TrimSpace(c.cfg.URL) == "" {
		return fmt.Errorf("vector store client: invalid empty URL")
	}

	if c.cfg.Provider == "mock" || strings.HasPrefix(c.cfg.URL, "mock://") {
		return nil
	}

	// Perform live HTTP ping for remote vector store endpoints
	reqURL := c.cfg.URL
	if !strings.HasPrefix(reqURL, "http://") && !strings.HasPrefix(reqURL, "https://") {
		reqURL = "http://" + reqURL
	}
	if !strings.HasSuffix(reqURL, "/health") && !strings.HasSuffix(reqURL, "/ping") {
		reqURL = strings.TrimSuffix(reqURL, "/") + "/health"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("vector store client: prepare health check request: %w", err)
	}

	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// If custom URL endpoint does not support /health, return error for bad connection
		return fmt.Errorf("vector store client: health check failed for %q: %w", c.cfg.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("vector store client: health check endpoint returned status %d", resp.StatusCode)
	}

	return nil
}

// BatchUpsert writes a batch of vector document inserts or updates to the store.
func (c *Client) BatchUpsert(ctx context.Context, collection string, docs []VectorDoc) error {
	if len(docs) == 0 {
		return nil
	}

	if c.cfg.Provider == "mock" || strings.HasPrefix(c.cfg.URL, "mock://") {
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

	// HTTP API batch write execution
	return c.sendHTTPRequest(ctx, http.MethodPost, collection, "/upsert", docs)
}

// BatchDelete removes vector documents by primary ID.
func (c *Client) BatchDelete(ctx context.Context, collection string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	if c.cfg.Provider == "mock" || strings.HasPrefix(c.cfg.URL, "mock://") {
		c.mu.Lock()
		defer c.mu.Unlock()

		if collection == "" {
			collection = "default"
		}
		if coll, ok := c.store[collection]; ok {
			for _, id := range ids {
				delete(coll, id)
			}
		}
		return nil
	}

	payload := map[string]interface{}{"ids": ids}
	return c.sendHTTPRequest(ctx, http.MethodPost, collection, "/delete", payload)
}

// DeleteAllDocuments clears all documents from a target collection for WriteReplace policy.
func (c *Client) DeleteAllDocuments(ctx context.Context, collection string) error {
	if c.cfg.Provider == "mock" || strings.HasPrefix(c.cfg.URL, "mock://") {
		c.mu.Lock()
		defer c.mu.Unlock()

		if collection == "" {
			collection = "default"
		}
		delete(c.store, collection)
		return nil
	}

	return c.sendHTTPRequest(ctx, http.MethodPost, collection, "/clear", map[string]string{"collection": collection})
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

func (c *Client) sendHTTPRequest(ctx context.Context, method, collection, path string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("vector store client: marshal payload: %w", err)
	}

	baseURL := c.cfg.URL
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	reqURL := fmt.Sprintf("%s/collections/%s%s", strings.TrimSuffix(baseURL, "/"), collection, path)

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("vector store client: create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("vector store client: send request to %q: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("vector store client: endpoint %q returned error status %d", reqURL, resp.StatusCode)
	}

	return nil
}
