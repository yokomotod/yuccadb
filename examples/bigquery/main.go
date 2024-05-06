package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/yokomotod/yuccadb"
	helper "github.com/yokomotod/yuccadb/helper/bigquery"
	"github.com/yokomotod/yuccadb/logger"
	"google.golang.org/api/googleapi"
)

const (
	dataDir   = "./data"
	datasetID = "yuccadb_example"
	tableID   = "sync_test"
)

func createBucketIfNotExists(ctx context.Context, client *storage.Client, bucketName string) error {
	log.Printf("Checking bucket %q exists\n", bucketName)
	bucket := client.Bucket(bucketName)
	_, err := bucket.Attrs(ctx)
	if err == nil {
		log.Printf("OK. Bucket %q already exists\n", bucketName)
		return nil
	}
	if err != storage.ErrBucketNotExist {
		return fmt.Errorf("bucket.Attrs: %w", err)
	}

	log.Printf("Creating bucket %q\n", bucketName)
	if err := bucket.Create(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), nil); err != nil {
		return fmt.Errorf("bucket.Create: %w", err)
	}

	return nil
}

func createDatasetIfNotExists(ctx context.Context, client *bigquery.Client) error {
	log.Printf("Checking dataset `%s.%s` exists\n", client.Project(), datasetID)
	ds := client.Dataset(datasetID)
	_, err := ds.Metadata(ctx)
	if err == nil {
		log.Printf("OK. Dataset `%s.%s` already exists\n", client.Project(), datasetID)
		return nil
	}

	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != 404 {
		return fmt.Errorf("dataset.Metadata: %w", err)
	}

	log.Printf("Creating dataset `%s.%s`\n", client.Project(), datasetID)
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
		return fmt.Errorf("ds.Create: %w", err)
	}

	return nil
}

func existsBQTable(ctx context.Context, client *bigquery.Client) (bool, error) {
	log.Printf("Checking BQ table `%s.%s.%s` exists\n", client.Project(), datasetID, tableID)
	_, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err == nil {
		log.Printf("OK. Table `%s.%s.%s` already exists\n", client.Project(), datasetID, tableID)
		return true, nil
	}

	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != 404 {
		return false, fmt.Errorf("table.Metadata: %w", err)
	}

	log.Printf("Table `%s.%s.%s` does not exist\n", client.Project(), datasetID, tableID)
	return false, nil
}

func updateBQTable(ctx context.Context, client *bigquery.Client) error {
	query := client.Query(`
		WITH
		  numbers AS (
		    SELECT id FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS id
		  ),
		  random_data AS (
		    SELECT
		      id,
		      CAST(FLOOR(RAND() * 1000000 + 1) AS INT64) AS random_int,
		      (
		        SELECT
		          STRING_AGG(
		            CASE
		              WHEN RAND() < 0.05 THEN CHR(10)  -- 約5%の確率で改行文字を挿入
		              ELSE CHR(32 + CAST(FLOOR(RAND() * 95) AS INT64))
		            END, '')
		        FROM
		          UNNEST(GENERATE_ARRAY(1, 10))  -- 10文字のランダムな文字列を生成
		      ) AS random_string
		    FROM
		      numbers
		  )
		SELECT
		  id,
		  random_int,
		  random_string
		FROM
		  random_data
		ORDER BY CAST(id AS STRING)`)
	query.Dst = client.Dataset(datasetID).Table(tableID)
	query.CreateDisposition = bigquery.CreateIfNeeded
	query.WriteDisposition = bigquery.WriteTruncate

	log.Printf("Creating/Updating table `%s.%s.%s`\n", client.Project(), datasetID, tableID)
	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("query.Run: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait: %w", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("status.Err(): %w", err)
	}

	log.Printf("Table `%s.%s.%s` updated\n", client.Project(), datasetID, tableID)
	return nil
}

type handler struct {
	db       *yuccadb.YuccaDB
	bqHelper *helper.BQHelper
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	res, err := h.db.GetValue(tableID, key)
	if err != nil {
		if err == yuccadb.ErrTableNotFound {
			http.Error(w, fmt.Sprintf("table not found: %q", tableID), http.StatusNotFound)
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
	ctx := r.Context()

	if err := updateBQTable(ctx, h.bqHelper.BQClient); err != nil {
		http.Error(w, fmt.Sprintf("updateBQTable: %v", err), http.StatusInternalServerError)
		return
	}
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
		// start := time.Now()

		responseWriter := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(responseWriter, r)

		// log.Printf("[ACCESS] %s %q %d %v", r.Method, r.URL.Path, responseWriter.status, time.Since(start))
	})
}

func run() error {
	ctx := context.Background()
	db := yuccadb.NewYuccaDB()
	db.Logger = &logger.DefaultLogger{Level: logger.Warning}

	gcsBucket := os.Getenv("GCS_BUCKET")
	bqHelper, err := helper.NewBQHelper(ctx, dataDir, "gs://"+gcsBucket)
	if err != nil {
		return fmt.Errorf("helper.NewBQHelper: %w", err)
	}

	log.Printf("Refreshing data directory %q\n", dataDir)
	if err := os.RemoveAll(dataDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("os.RemoveAll: %w", err)
	}
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		return fmt.Errorf("os.MkdirAll: %w", err)
	}

	if err := createBucketIfNotExists(ctx, bqHelper.GCSClient, gcsBucket); err != nil {
		return fmt.Errorf("createBucketIfNotExists: %w", err)
	}

	if err := createDatasetIfNotExists(ctx, bqHelper.BQClient); err != nil {
		return fmt.Errorf("createDatasetIfNotExists: %w", err)
	}

	exists, err := existsBQTable(ctx, bqHelper.BQClient)
	if err != nil {
		return fmt.Errorf("existsBQTable: %w", err)
	}
	if !exists {
		if err := updateBQTable(ctx, bqHelper.BQClient); err != nil {
			return fmt.Errorf("updateBQTable: %w", err)
		}
	}

	tableMapping := helper.TableMapping{
		BQFullTableID: datasetID + "." + tableID,
		DBTableName:   tableID,
	}

	log.Printf("Starting sync tables: %q -> %q\n", tableMapping.BQFullTableID, tableMapping.DBTableName)
	ch, err := bqHelper.StartSyncTables(ctx, db, []helper.TableMapping{tableMapping})
	if err != nil {
		return fmt.Errorf("bqHelper.ImportTables: %w", err)
	}
	go (func() {
		for err := range ch {
			log.Printf("sync error: %v", err)
		}
	})()

	h := &handler{db: db, bqHelper: bqHelper}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/{key}", h.get)
	mux.HandleFunc("GET /v1/{$}", h.get)
	mux.HandleFunc("POST /v1/_update", h.put)

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
