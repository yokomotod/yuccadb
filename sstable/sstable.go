package sstable

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	defaultIndexInterval = 1_000
	expectedCols         = 2
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

func NewSSTable(csvFile string) (*SSTable, error) {
	table := &SSTable{
		indexInterval: defaultIndexInterval,
	}

	err := table.load(csvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	return table, nil
}

func (t *SSTable) load(csvFile string) error {
	file, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %s, %w", csvFile, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var count, offset, lastOffset int

	var lastKey string

	index := make([]indexEntry, 0)

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, ",")

		if len(cols) != expectedCols {
			return fmt.Errorf("invalid line: %s", line)
		}

		key := cols[0]

		if count%t.indexInterval == 0 {
			// log.Printf("Offset: %d Line: %s\n", offset, line)
			index = append(index, indexEntry{key, int64(offset)})
		}

		lastOffset = offset
		offset += len(line) + 1
		count++
		lastKey = key
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %w", err)
	}

	// add last key
	if index[len(index)-1].key != lastKey {
		index = append(index, indexEntry{lastKey, int64(lastOffset)})
	}

	t.File = csvFile
	t.index = index

	log.Printf("Loaded %s, %d items\n", csvFile, count)

	return nil
}

type Profile struct {
	SearchOffset time.Duration
	Open         time.Duration
	Seek         time.Duration
	Scan         time.Duration
}

type Result struct {
	Value     string
	KeyExists bool
	Profile   Profile
}

func (t *SSTable) Get(key string) (Result, error) {
	profile := Profile{}
	time1 := time.Now()

	offset, limit := t.searchOffset(key)

	time2 := time.Now()
	profile.SearchOffset = time2.Sub(time1)
	time1 = time2

	if offset == -1 {
		// log.Printf("Not found offset: %v\n", key)
		return Result{"", false, profile}, nil
	}

	// log.Printf("Found offset: %v, limit: %v, for %v\n", offset, limit, key)

	file, err := os.Open(t.File)
	if err != nil {
		return Result{"", false, profile}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	time2 = time.Now()
	profile.Open = time2.Sub(time1)
	time1 = time2

	_, err = file.Seek(offset, 0)
	if err != nil {
		return Result{"", false, profile}, fmt.Errorf("failed to seek file: %w", err)
	}

	time2 = time.Now()
	profile.Seek = time2.Sub(time1)
	time1 = time2

	value, keyExists, err := t.scan(file, key, offset, limit)
	if err != nil {
		return Result{"", false, profile}, fmt.Errorf("failed to scan file: %w", err)
	}

	time2 = time.Now()
	profile.Scan = time2.Sub(time1)

	return Result{value, keyExists, profile}, nil
}

func (t *SSTable) searchOffset(key string) (offset, limit int64) {
	idx := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].key >= key
	})

	if idx >= len(t.index) {
		return -1, -1
	}

	if t.index[idx].key == key {
		return t.index[idx].offset, t.index[idx].offset
	}

	return t.index[idx-1].offset, t.index[idx].offset
}

func (t *SSTable) scan(f *os.File, key string, offset, limit int64) (string, bool, error) {
	var scannedLines, scannedBytes int

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// log.Printf("Read: %s\n", line)

		// split line and return value
		cols := strings.Split(line, ",")
		if len(cols) != expectedCols {
			return "", false, fmt.Errorf("invalid line: %s", line)
		}

		if cols[0] == key {
			return cols[1], true, nil
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

	if err := scanner.Err(); err != nil {
		return "", false, fmt.Errorf("failed to scan file: %w", err)
	}

	// should never happen
	return "", false, errors.New("should never reach here")
}
