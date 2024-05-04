package bigquery

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

func splitGCSPath(gcsPath string) (bucketName, prefix string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid gcsPath: %s", gcsPath)
	}

	path := strings.TrimPrefix(gcsPath, "gs://")
	slashIndex := strings.Index(path, "/")
	if slashIndex == -1 {
		return path, "", nil
	}

	bucketName = path[:slashIndex]
	prefix = path[slashIndex+1:]
	return bucketName, prefix, nil
}

func extractToGCS(ctx context.Context, datasetID, tableID, gcsURI string) error {
	bqClient, err := bigquery.NewClient(ctx, bigquery.DetectProjectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer bqClient.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.Compression = bigquery.Gzip
	gcsRef.DestinationFormat = bigquery.CSV

	extractor := bqClient.Dataset(datasetID).Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = true

	job, err := extractor.Run(ctx)
	if err != nil {
		return fmt.Errorf("extractor.Run: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("status.Err(): %w", status.Err())
	}

	return nil
}

// gcsPath: gs://bucket-name/prefix
func downloadCSV(ctx context.Context, gcsBucket, gcsObject, destPath string) error {

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer gcsClient.Close()

	f, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("os.Create: %v", err)
	}
	defer (func() {
		if err := f.Close(); err != nil {
			log.Fatalf("f.Close: %v", err)
		}
	})()

	rc, err := gcsClient.Bucket(gcsBucket).Object(gcsObject).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %v", gcsObject, err)
	}
	defer rc.Close()

	gr, err := gzip.NewReader(rc)
	if err != nil {
		return fmt.Errorf("gzip.NewReader: %v", err)
	}
	defer gr.Close()

	if _, err := io.Copy(f, gr); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	return nil
}

func DownloadTableCSV(ctx context.Context, datasetID, tableID, gcsPath, destPath string) error {
	gcsBucket, gcsPrefix, err := splitGCSPath(gcsPath)
	if err != nil {
		return fmt.Errorf("splitGCSPath: %w", err)
	}

	gcsURI := gcsPath + "/" + datasetID + "-" + tableID + "-*.csv.gz"
	gcsObject := datasetID + "-" + tableID + "-000000000000.csv.gz"
	if gcsPrefix != "" {
		gcsObject = gcsPrefix + "/" + gcsObject
	}

	err = extractToGCS(ctx, datasetID, tableID, gcsURI)
	if err != nil {
		return fmt.Errorf("extractToGCS: %w", err)
	}

	err = downloadCSV(ctx, gcsBucket, gcsObject, destPath)
	if err != nil {
		return fmt.Errorf("downloadCSV: %w", err)
	}

	return nil
}
