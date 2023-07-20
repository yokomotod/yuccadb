package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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
	got1 := request(t, http.MethodPut, s.URL+"/tables/test", payload)
	wont1 := map[string]interface{}{"message": "test table is created"}
	if !reflect.DeepEqual(got1, wont1) {
		t.Fatalf("expected json is %v, but got %v", wont1, got1)
	}

	got2 := request(t, http.MethodGet, s.URL+"/tables", nil)
	wont2 := map[string]interface{}{"tables": []interface{}{"test"}}
	if !reflect.DeepEqual(got2, wont2) {
		t.Fatalf("expected json is %v, but got %v", wont2, got2)
	}

	got3 := request(t, http.MethodGet, s.URL+"/tables/test/key", nil)
	wont3 := map[string]interface{}{"value": "value"}
	if !reflect.DeepEqual(wont3, got3) {
		t.Fatalf("expected json is %v, but got %v", wont3, got3)
	}
}

func request(t *testing.T, method, url string, payload map[string]string) map[string]interface{} {
	t.Helper()

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

	var j map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&j); err != nil {
		t.Fatalf("json decode err should be nil: %v", err)
	}
	return j
}
