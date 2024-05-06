package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yokomotod/yuccadb"
	"github.com/yokomotod/yuccadb/logger"
)

type handler struct {
	db *yuccadb.YuccaDB
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	tableName, key := r.PathValue("table"), r.PathValue("key")

	res, err := h.db.GetValue(tableName, key)
	if err != nil {
		if err == yuccadb.ErrTableNotFound {
			http.Error(w, fmt.Sprintf("table not found: %q", tableName), http.StatusNotFound)
			return
		}

		http.Error(w, fmt.Sprintf("db.GetValue: %v", err), http.StatusInternalServerError)
		return
	}

	if res.Values == nil {
		http.Error(w, fmt.Sprintf("key not found: %q", key), http.StatusNotFound)
		return
	}

	fmt.Fprint(w, res.Values)
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	csvFilePath := r.FormValue("file")

	if err := h.db.PutTable(tableName, csvFilePath, true); err != nil {
		http.Error(w, fmt.Sprintf("db.PutTable: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "OK")
}

// HTTPステータスコードを記録するためのラッパー
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		responseWriter := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(responseWriter, r)

		log.Printf("[ACCESS] %s %q %d %v", r.Method, r.URL.Path, responseWriter.status, time.Since(start))
	})
}

func run() error {
	db := yuccadb.NewYuccaDB()
	db.Logger = &logger.DefaultLogger{Level: logger.Trace}

	h := &handler{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/{table}/{key}", h.get)
	mux.HandleFunc("GET /v1/{table}/{$}", h.get)
	mux.HandleFunc("PUT /v1/{table}", h.put)

	loggedMux := loggingMiddleware(mux)

	log.Println("Starting HTTP server on port 8080")

	if err := http.ListenAndServe(":8080", loggedMux); err != nil {
		return fmt.Errorf("http.ListenAndServe: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
