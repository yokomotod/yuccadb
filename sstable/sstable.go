package sstable

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
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

func NewSSTable(ctx context.Context, name, csvFile string) (*SSTable, error) {
	t := &SSTable{
		indexInterval: 1_000,
	}

	err := t.load(ctx, name, csvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %s", err)
	}

	return t, nil
}

func (t *SSTable) load(ctx context.Context, tableName, csvFile string) error {
	f, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %s, %s", csvFile, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	count, offset := 0, 0
	index := make([]indexEntry, 0)

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, ",")

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

	t.File = csvFile
	t.index = index

	fmt.Printf("Loaded %s, %d items\n", csvFile, count)
	return nil
}

type Profile struct {
	SearchOffset time.Duration
	Open         time.Duration
	Seek         time.Duration
	Scan         time.Duration
}

type result struct {
	Value     string
	KeyExists bool
	Profile   Profile
}

func (t *SSTable) Get(key string) (result, error) {
	p := Profile{}
	t1 := time.Now()

	offset, limit := t.searchOffset(key)

	t2 := time.Now()
	p.SearchOffset = t2.Sub(t1)
	t1 = t2

	if offset == -1 {
		// fmt.Printf("Not found offset: %v\n", key)
		return result{"", false, p}, nil
	}

	// fmt.Printf("Found offset: %v for %v\n", t.index[i].offset, t.index[i].key)

	// open file and seek to offset
	f, err := os.Open(t.File)
	if err != nil {
		return result{"", false, p}, fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	t2 = time.Now()
	p.Open = t2.Sub(t1)
	t1 = t2

	_, err = f.Seek(offset, 0)
	if err != nil {
		return result{"", false, p}, fmt.Errorf("failed to seek file: %s", err)
	}

	t2 = time.Now()
	p.Seek = t2.Sub(t1)
	t1 = t2

	var scannedLines, scannedBytes int
	var value string

	// read line
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// fmt.Printf("Read: %s\n", line)

		// split line and return value
		cols := strings.Split(line, ",")
		if len(cols) != 2 {
			return result{"", false, p}, fmt.Errorf("invalid line: %s", line)
		}

		if cols[0] == key {
			value = cols[1]
			break
		}

		scannedLines++
		scannedBytes += len(line) + 1

		if offset+int64(scannedBytes) >= limit {
			// reached to next index, means not found
			return result{"", false, p}, nil
		}

		if scannedLines > t.indexInterval {
			// should never happen
			return result{"", false, p}, fmt.Errorf("too many scanned lines: %d", scannedLines)
		}
	}

	if scanner.Err() != nil {
		return result{"", false, p}, fmt.Errorf("failed to scan file: %s", err)
	}

	t2 = time.Now()
	p.Scan = t2.Sub(t1)
	t1 = t2

	return result{value, true, p}, nil
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
