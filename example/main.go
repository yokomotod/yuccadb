package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/yokomotod/yuccadb"
	helper "github.com/yokomotod/yuccadb/helper/bigquery"
)

func prepareTable(ctx context.Context) (datasetID, tableID string, err error) {
	// Create a new BigQuery bqClient
	bqClient, err := bigquery.NewClient(ctx, bigquery.DetectProjectID)
	if err != nil {
		return "", "", fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer bqClient.Close()

	// // Create the dataset if it does not exist
	// if err := createDatasetIfNotExists(ctx, client, TmpDatasetID); err != nil {
	// 	log.Fatalf("Failed to create dataset: %w", err)
	// }
	// expiration := time.Now().Add(24 * time.Hour)
	// tableRef := client.Dataset(TmpDatasetID).Table(fmt.Sprintf("temp_results_%d", time.Now().Unix()))
	// metadata := &bigquery.TableMetadata{
	// 	ExpirationTime: expiration,
	// }
	// if err := client.Dataset(TmpDatasetID).Table(tableRef.TableID).Create(ctx, metadata); err != nil {
	// 	fmt.Printf("Failed to create table with expiration: %w", err)
	// 	return
	// }

	// Construct the query
	query := bqClient.Query(`
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
	// query.DestinationTable = tableRef
	// query.CreateDisposition = bigquery.CreateIfNeeded
	// query.WriteDisposition = bigquery.WriteTruncate

	// Run the query
	job, err := query.Run(ctx)
	if err != nil {
		return "", "", fmt.Errorf("query.Run: %w", err)
	}

	config, err := job.Config()
	if err != nil {
		return "", "", fmt.Errorf("job.Config: %w", err)
	}

	queryConfig, ok := config.(*bigquery.QueryConfig)
	if !ok {
		return "", "", fmt.Errorf("config.(*bigquery.QueryConfig): %w", err)
	}

	// Wait for the query to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return "", "", fmt.Errorf("job.Wait: %w", err)
	}

	if err := status.Err(); err != nil {
		return "", "", fmt.Errorf("status.Err(): %w", err)
	}

	// // Download the result as CSV
	// it, err := job.Read(ctx)
	// if err != nil {
	// 	return fmt.Errorf("Failed to read query result: %w", err)
	// }
	// for {
	// 	var row []bigquery.Value
	// 	err := it.Next(&row)
	// 	if err == iterator.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		return fmt.Errorf("Failed to read row: %w", err)
	// 	}
	// 	fmt.Printf("Row: %w", row)
	// }

	return queryConfig.Dst.DatasetID, queryConfig.Dst.TableID, nil
}

func replaceTable(db *yuccadb.YuccaDB, tableName string) error {
	log.Print("Start replaceTable")
	ctx := context.Background()

	datasetID, tableID, err := prepareTable(ctx)
	if err != nil {
		return fmt.Errorf("prepareTable: %w", err)
	}

	log.Println("Query completed successfully")

	csvPath := tableName + ".csv"
	if err := helper.DownloadTableCSV(ctx, datasetID, tableID, os.Getenv("GCS_EXTRACT_PATH"), csvPath); err != nil {
		return fmt.Errorf("downloadCSV: %w", err)
	}

	log.Println("Downloaded CSV")

	err = db.PutTable(tableName, csvPath, false)
	if err != nil {
		return fmt.Errorf("PutTable: %w", err)
	}

	return nil
}

type handler struct {
	db *yuccadb.YuccaDB
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	key := r.PathValue("key")

	log.Printf("get table: %s, key: %s", table, key)
	res, err := h.db.GetValue(table, key)
	if err != nil {
		if err == yuccadb.ErrTableNotFound {
			http.Error(w, fmt.Sprintf("table not found: %s", table), http.StatusNotFound)
			return
		}

		http.Error(w, fmt.Sprintf("db.GetValue: %v", err), http.StatusInternalServerError)
		return
	}

	if res.Values == nil {
		http.Error(w, fmt.Sprintf("key not found: %s", key), http.StatusNotFound)
		return
	}

	fmt.Fprint(w, res.Values)
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")

	if err := replaceTable(h.db, table); err != nil {
		http.Error(w, fmt.Sprintf("replaceTable: %v", err), http.StatusInternalServerError)
		return
	}
}

func run() error {
	db := yuccadb.NewYuccaDB()
	h := &handler{db: db}

	http.HandleFunc("GET /v1/{table}/{key}", h.get)
	http.HandleFunc("GET /v1/{table}/{$}", h.get)
	http.HandleFunc("POST /v1/{table}", h.put)

	log.Println("Starting HTTP server on port 8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		return fmt.Errorf("http.ListenAndServe: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
