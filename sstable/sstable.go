package sstable

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/yokomotod/yuccadb/logger"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
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
	indexInterval int64
	Logger        logger.Logger
}

func NewSSTable(csvFile string, logger logger.Logger) (*SSTable, error) {
	table := &SSTable{
		indexInterval: defaultIndexInterval,
		Logger:        logger,
	}

	err := table.load(csvFile)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}

	return table, nil
}

func (t *SSTable) load(csvFile string) error {
	time0 := time.Now()

	file, err := os.Open(csvFile)
	if err != nil {
		return fmt.Errorf("os.Open(%q): %w", csvFile, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var count, lastOffset int64

	var lastKey string

	index := make([]indexEntry, 0)

	for {
		offset := reader.InputOffset()

		cols, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("csv.Reader.Read: %w", err)
		}

		key := cols[0]
		if key < lastKey {
			return fmt.Errorf("keys are not sorted: %q, %q", lastKey, key)
		}

		if count%t.indexInterval == 0 {
			index = append(index, indexEntry{key, offset})
		}

		lastOffset = offset
		count++
		lastKey = key
	}

	// add last key
	if index[len(index)-1].key != lastKey {
		index = append(index, indexEntry{lastKey, lastOffset})
	}

	t.File = csvFile
	t.index = index

	time1 := time.Now()
	p := message.NewPrinter(language.English)
	t.Logger.Infof(p.Sprintf("Loaded %q with %d items (%s)", csvFile, count, time1.Sub(time0).String()))

	return nil
}

type Profile struct {
	SearchOffset time.Duration
	Open         time.Duration
	Seek         time.Duration
	Scan         time.Duration
}

type Result struct {
	Values  []string
	Profile Profile
}

func (t *SSTable) Get(key string) (Result, error) {
	profile := Profile{}
	time1 := time.Now()

	offset, limit := t.searchOffset(key)

	time2 := time.Now()
	profile.SearchOffset = time2.Sub(time1)
	time1 = time2

	if offset == -1 {
		return Result{nil, profile}, nil
	}

	t.Logger.Tracef("Found offset: %v, limit: %v, for %v\n", offset, limit, key)

	file, err := os.Open(t.File)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("os.Open(%q): %w", t.File, err)
	}
	defer file.Close()

	time2 = time.Now()
	profile.Open = time2.Sub(time1)
	time1 = time2

	_, err = file.Seek(offset, 0)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("file.Seek: %w", err)
	}

	time2 = time.Now()
	profile.Seek = time2.Sub(time1)
	time1 = time2

	value, err := t.scanFile(file, key, offset, limit)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("scanFile: %w", err)
	}

	time2 = time.Now()
	profile.Scan = time2.Sub(time1)

	return Result{value, profile}, nil
}

func (t *SSTable) searchOffset(key string) (offset, limit int64) {
	idx := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].key >= key
	})

	if idx >= len(t.index) {
		t.Logger.Tracef("Offset not found for %v, greater than last key %v\n", key, t.index[len(t.index)-1].key)

		return -1, -1
	}

	if t.index[idx].key == key {
		t.Logger.Tracef("Offset found for %v, at %v\n", key, t.index[idx].offset)

		return t.index[idx].offset, t.index[idx].offset
	}

	if idx == 0 {
		t.Logger.Tracef("Offset not found for %v, less than first key %v\n", key, t.index[0].key)

		return -1, -1
	}

	return t.index[idx-1].offset, t.index[idx].offset
}

func (t *SSTable) scanFile(f *os.File, key string, offset, limit int64) ([]string, error) {
	var scannedLines int64

	reader := csv.NewReader(f)

	for {
		cols, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		if cols[0] == key {
			return cols[1:], nil
		}

		scannedLines++

		if offset+reader.InputOffset() >= limit {
			// reached to next index, means not found
			return nil, nil
		}

		if scannedLines > t.indexInterval {
			// should never happen
			return nil, fmt.Errorf("too many scanned lines: %d", scannedLines)
		}
	}

	// should never happen, last key should be in index
	return nil, errors.New("should never reach here")
}
