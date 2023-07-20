package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestServerRequest(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	content := "key\tvalue"
	testFile := filepath.Join(tempDir, "test.tsv")
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	ss, err := NewServer(ctx, tempDataDir)
	if err != nil {
		t.Fatalf("failed to create server: %s", err)
	}
	s := httptest.NewServer(ss.setupRouter())

	payload := map[string]string{"file": testFile}
	j1 := request(t, http.MethodPut, s.URL+"/tables/test", payload)
	if j1["message"] != "test table is created" {
		t.Fatalf("expected message is test table is created, but got %v", j1)
	}

	j2 := request(t, http.MethodGet, s.URL+"/tables/test/key", nil)
	if j2["value"] != "value" {
		t.Fatalf("expected value is value, but got %v", j2)
	}
}

func request(t *testing.T, method, url string, payload map[string]string) map[string]string {
	payloadJson, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %s", err)
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(payloadJson))
	if err != nil {
		t.Fatalf("failed to create request: %s", err)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		t.Fatalf("status code should be 200, but %d", r.StatusCode)
	}

	var j map[string]string
	if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
		t.Fatalf("json decode err should be nil: %v", err)
	}
	return j
}
