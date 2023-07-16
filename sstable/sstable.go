package sstable

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
)

type indexEntry struct {
	key    string
	offset int64
}

type SSTable struct {
	file          string
	index         []indexEntry
	indexInterval int
}

func NewSSTable(ctx context.Context, tsvFile string) (*SSTable, error) {
	t := &SSTable{
		indexInterval: 1_000,
	}

	err := t.load(ctx, tsvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load file: %s", err)
	}

	return t, nil
}

func (t *SSTable) load(ctx context.Context, tsvFile string) error {
	if strings.HasPrefix(tsvFile, "gs://") {
		var err error
		tsvFile, err = t.download(ctx, tsvFile)
		if err != nil {
			return fmt.Errorf("failed to download file: %s", err)
		}
	}

	f, err := os.Open(tsvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %s, %s", tsvFile, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	i, offset := 0, 0

	t.index = make([]indexEntry, 0)

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, "\t")
		key := cols[0]

		if i%t.indexInterval == 0 {
			// fmt.Printf("Offset: %d Line: %s\n", offset, line)
			t.index = append(t.index, indexEntry{key, int64(offset)})
		}

		offset += len(line) + 1
		i++
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %s", err)
	}

	t.file = tsvFile

	return nil
}

func (t *SSTable) download(ctx context.Context, gcsPath string) (string, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	pathSegments := strings.Split(gcsPath, "/")

	bucket := pathSegments[2]
	object := strings.Join(pathSegments[3:], "/")
	basename := pathSegments[len(pathSegments)-1]

	f, err := os.Create(basename)
	if err != nil {
		return "", fmt.Errorf("os.Create: %w", err)
	}
	defer f.Close()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return "", fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	fmt.Printf("Downloading %v from %v\n", object, bucket)
	if _, err := io.Copy(f, rc); err != nil {
		return "", fmt.Errorf("io.Copy: %w", err)
	}
	fmt.Printf("Downloaded %v\n", object)

	return basename, nil
}

func (t *SSTable) Get(key string) (value string, keyExists bool, err error) {
	offset, limit := t.searchOffset(key)

	if offset == -1 {
		// fmt.Printf("Not found offset: %v\n", key)
		return "", false, nil
	}

	// fmt.Printf("Found offset: %v for %v\n", t.index[i].offset, t.index[i].key)

	// open file and seek to offset
	f, err := os.Open(t.file)
	if err != nil {
		return "", false, fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return "", false, fmt.Errorf("failed to seek file: %s", err)
	}

	scannedLines, scannedBytes := 0, 0

	// read line
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// fmt.Printf("Read: %s\n", line)

		// split line and return value
		cols := strings.Split(line, "\t")
		if len(cols) != 2 {
			return "", false, fmt.Errorf("invalid line: %s", line)
		}

		if cols[0] == key {
			value = cols[1]
			break
		}

		scannedLines++
		scannedBytes += len(line) + 1

		if offset+int64(scannedBytes) >= limit {
			// reached to next index, means not found
			return "", false, nil
		}

		if scannedLines > t.indexInterval {
			// should never happen
			return "", false, fmt.Errorf("too many scanned lines: %d", scannedLines)
		}
	}

	if scanner.Err() != nil {
		return "", false, fmt.Errorf("failed to scan file: %s", err)
	}

	return value, true, nil
}

func (t *SSTable) searchOffset(key string) (offset, limit int64) {
	i := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].key >= key
	})

	if i >= len(t.index) {
		return -1, -1
	}

	if t.index[i].key == key {
		return t.index[i].offset, t.index[i].offset
	}

	return t.index[i-1].offset, t.index[i].offset
}
