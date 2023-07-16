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
	dataDir       string
	file          string
	index         []indexEntry
	indexInterval int
}

func NewSSTable(ctx context.Context, name, tsvFile, dataDir string) (*SSTable, error) {
	t := &SSTable{
		dataDir:       dataDir,
		indexInterval: 1_000,
	}

	err := t.load(ctx, name, tsvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %s", err)
	}

	return t, nil
}

func (t *SSTable) load(ctx context.Context, tableName, srcFile string) error {
	localFile := fmt.Sprintf("%s/%s.tsv", t.dataDir, tableName)
	tmpFile := fmt.Sprintf("%s.tmp", localFile)
	if localFile == srcFile {
		tmpFile = localFile
	}

	index, count, err := tryLoad(ctx, srcFile, tmpFile, t.indexInterval)
	if err != nil {
		if tmpFile != localFile {
			_ = os.Remove(tmpFile)
		}
		return fmt.Errorf("failed to try load: %s", err)
	}

	err = os.Rename(tmpFile, localFile)
	if err != nil {
		return fmt.Errorf("failed to rename file: %s", err)
	}

	t.index = index
	t.file = localFile

	fmt.Printf("Loaded %s, %d items\n", localFile, count)
	return nil
}

func tryLoad(ctx context.Context, srcFile, tmpFile string, indexInterval int) (index []indexEntry, count int, err error) {
	if strings.HasPrefix(srcFile, "gs://") {
		err := download(ctx, tmpFile, srcFile)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to download file: %s", err)
		}
	} else {
		err := copy(tmpFile, srcFile)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to copy file: %s", err)
		}
	}

	f, err := os.Open(tmpFile)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %s, %s", tmpFile, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	offset := 0

	index = make([]indexEntry, 0)

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, "\t")

		if len(cols) != 2 {
			return nil, 0, fmt.Errorf("invalid line: %s", line)
		}

		key := cols[0]

		if count%indexInterval == 0 {
			// fmt.Printf("Offset: %d Line: %s\n", offset, line)
			index = append(index, indexEntry{key, int64(offset)})
		}

		offset += len(line) + 1
		count++
	}

	if scanner.Err() != nil {
		return nil, 0, fmt.Errorf("failed to scan file: %s", err)
	}

	return index, count, nil
}

func download(ctx context.Context, localFile, gcsPath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	f, err := os.Create(localFile)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}
	defer f.Close()

	pathSegments := strings.Split(gcsPath, "/")
	bucket := pathSegments[2]
	object := strings.Join(pathSegments[3:], "/")

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	fmt.Printf("Downloading %v from %v\n", object, bucket)
	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	fmt.Printf("Downloaded %v\n", object)

	return nil
}

func copy(localFile, srcPath string) error {
	if localFile == srcPath {
		fmt.Printf("Skip copying %v\n", srcPath)
		return nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}
	defer src.Close()

	dst, err := os.Create(localFile)
	if err != nil {
		return fmt.Errorf("failed to create file: %s", err)
	}
	defer dst.Close()

	fmt.Printf("Copying %v to %v\n", srcPath, localFile)
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to copy file: %s", err)
	}

	fmt.Printf("Copied %v\n", srcPath)

	return nil
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
