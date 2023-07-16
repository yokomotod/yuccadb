package sstable

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
)

type indexEntry struct {
	key    string
	offset int64
}

type SSTable struct {
	File          string
	index         []indexEntry
	indexInterval int
}

func NewSSTable(ctx context.Context, name, tsvFile string) (*SSTable, error) {
	t := &SSTable{
		indexInterval: 1_000,
	}

	err := t.load(ctx, name, tsvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %s", err)
	}

	return t, nil
}

func (t *SSTable) load(ctx context.Context, tableName, tsvFile string) error {
	f, err := os.Open(tsvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %s, %s", tsvFile, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	count, offset := 0, 0
	index := make([]indexEntry, 0)

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, "\t")

		if len(cols) != 2 {
			return fmt.Errorf("invalid line: %s", line)
		}

		key := cols[0]

		if count%t.indexInterval == 0 {
			// fmt.Printf("Offset: %d Line: %s\n", offset, line)
			index = append(index, indexEntry{key, int64(offset)})
		}

		offset += len(line) + 1
		count++
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %s", err)
	}

	t.File = tsvFile
	t.index = index

	fmt.Printf("Loaded %s, %d items\n", tsvFile, count)
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
	f, err := os.Open(t.File)
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
