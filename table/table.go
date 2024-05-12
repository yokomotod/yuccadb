package table

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/yokomotod/yuccadb/internals/humanize"
	"github.com/yokomotod/yuccadb/logger"
)

const (
	defaultIndexInterval = 1_000
)

type indexEntry struct {
	key    string
	offset int64
}

type Table struct {
	File          string
	index         []indexEntry
	indexInterval int64
	Logger        logger.Logger
}

func BuildTable(csvFile string, logger logger.Logger) (*Table, error) {
	table := &Table{
		indexInterval: defaultIndexInterval,
		Logger:        logger,
	}

	err := table.load(csvFile)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}

	return table, nil
}

func (t *Table) load(csvFile string) error {
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
	t.Logger.Infof("Loaded %q with %d items (%v)", csvFile, humanize.Comma(count), time1.Sub(time0))

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

func (t *Table) Get(key string) (Result, error) {
	profile := Profile{}
	time1 := time.Now()

	offset, limit := t.searchIndex(key)

	time2 := time.Now()
	profile.SearchOffset = time2.Sub(time1)
	time1 = time2

	if offset == -1 {
		return Result{nil, profile}, nil
	}

	file, err := os.Open(t.File)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("os.Open(%q): %w", t.File, err)
	}
	defer file.Close()

	time2 = time.Now()
	profile.Open = time2.Sub(time1)
	time1 = time2

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

func (t *Table) searchIndex(key string) (offset, limit int64) {
	idx := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].key >= key
	})

	if idx >= len(t.index) {
		t.Logger.Tracef("Offset not found for %v, greater than last key %v\n", key, t.index[len(t.index)-1].key)

		return -1, -1
	}

	if t.index[idx].key == key {
		t.Logger.Tracef("Found exact offset=limit=%v for %v\n", offset, limit, key)

		return t.index[idx].offset, t.index[idx].offset
	}

	if idx == 0 {
		t.Logger.Tracef("Offset not found for %v, less than first key %v\n", key, t.index[0].key)

		return -1, -1
	}

	t.Logger.Tracef("Found range offset=%v, limit=%v for %v\n", offset, limit, key)

	return t.index[idx-1].offset, t.index[idx].offset
}

func (t *Table) scanFile(f *os.File, key string, offset, limit int64) ([]string, error) {
	var scannedLines int64

	reader := csv.NewReader(f)

	_, err := f.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("file.Seek: %w", err)
	}

	for {
		cols, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("csv.Reader.Read: %w", err)
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
