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
	"github.com/yokomotod/yuccadb/logger"
)

func splitGCSPath(gcsPath string) (bucketName, prefix string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid gcsPath: %q", gcsPath)
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

type BQHelper struct {
	BQClient    *bigquery.Client
	GCSClient   *storage.Client
	DownloadDir string
	GCSPath     string
	gcsBucket   string
	gcsPrefix   string
	Logger      logger.Logger
}

func NewBQHelper(ctx context.Context, downloadDir, gcsPath string) (*BQHelper, error) {
	bqClient, err := bigquery.NewClient(ctx, bigquery.DetectProjectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}

	gcsBucket, gcsPrefix, err := splitGCSPath(gcsPath)
	if err != nil {
		return nil, err
	}

	return &BQHelper{
		BQClient:    bqClient,
		GCSClient:   gcsClient,
		DownloadDir: downloadDir,
		GCSPath:     gcsPath,
		gcsBucket:   gcsBucket,
		gcsPrefix:   gcsPrefix,
		Logger:      &logger.DefaultLogger{},
	}, nil
}

func (h *BQHelper) Close() error {
	if err := h.BQClient.Close(); err != nil {
		return fmt.Errorf("BQClient.Close: %w", err)
	}

	if err := h.GCSClient.Close(); err != nil {
		return fmt.Errorf("GCSClient.Close: %w", err)
	}

	return nil
}

func (h *BQHelper) splitFullTableID(fullTableID string) (projectID, datasetID, tableID string, err error) {
	parts := strings.Split(fullTableID, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid full table ID format")
	}

	if len(parts) == 2 {
		projectID := h.BQClient.Project()
		if projectID == "" {
			return "", "", "", fmt.Errorf("failed to detect project ID")
		}

		return projectID, parts[0], parts[1], nil
	}

	return parts[0], parts[1], parts[2], nil
}

func (h *BQHelper) extractToGCS(ctx context.Context, projectID, datasetID, tableID, gcsURI string) error {
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.Compression = bigquery.Gzip
	gcsRef.DestinationFormat = bigquery.CSV

	extractor := h.BQClient.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = true

	h.Logger.Debugf("Extracting table `%s.%s.%s` to %q\n", projectID, datasetID, tableID, gcsURI)

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

	h.Logger.Debugf("Extracted table `%s.%s.%s` to %q\n", projectID, datasetID, tableID, gcsURI)

	return nil
}

// gcsPath: gs://bucket-name/prefix
func (h *BQHelper) downloadCSV(ctx context.Context, gcsObject, destPath string) error {
	h.Logger.Debugf("Downloading %q to %q\n", gcsObject, destPath)

	f, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("os.OpenFile(%q): %v", destPath, err)
	}
	defer (func() {
		if err := f.Close(); err != nil {
			log.Fatalf("f.Close: %v", err)
		}
	})()

	rc, err := h.GCSClient.Bucket(h.gcsBucket).Object(gcsObject).NewReader(ctx)
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

	h.Logger.Debugf("Downloaded %q to %q\n", gcsObject, destPath)

	return nil
}

func (h *BQHelper) DownloadTableCSV(ctx context.Context, fullTableID, filename string) error {
	projectID, datasetID, tableID, err := h.splitFullTableID(fullTableID)
	if err != nil {
		return fmt.Errorf("splitTableID: %w", err)
	}

	gcsURI := h.GCSPath + "/" + fullTableID + "-*.csv.gz"
	gcsObject := fullTableID + "-000000000000.csv.gz"
	if h.gcsPrefix != "" {
		gcsObject = h.gcsPrefix + "/" + gcsObject
	}

	err = h.extractToGCS(ctx, projectID, datasetID, tableID, gcsURI)
	if err != nil {
		return fmt.Errorf("extractToGCS: %w", err)
	}

	err = h.downloadCSV(ctx, gcsObject, h.DownloadDir+"/"+filename)
	if err != nil {
		return fmt.Errorf("downloadCSV: %w", err)
	}

	return nil
}
