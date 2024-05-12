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
	file          string
	index         []indexEntry
	timestamp     time.Time
	indexInterval int64
	Logger        logger.Logger
}

func (t *Table) File() string {
	return t.file
}

func (t *Table) Timestamp() time.Time {
	return t.timestamp
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
	reader.ReuseRecord = true

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

	t.file = csvFile
	t.index = index
	t.timestamp = time.Now()

	t.Logger.Infof("Loaded %q with %d items (%v)", csvFile, humanize.Comma(count), t.timestamp.Sub(time0))

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

	file, err := os.Open(t.file)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("os.Open(%q): %w", t.file, err)
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

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	value, err := t.scanFile(reader, key, limit-offset)
	if err != nil {
		return Result{nil, profile}, fmt.Errorf("scanFile: %w", err)
	}

	time2 = time.Now()
	profile.Scan = time2.Sub(time1)

	return Result{value, profile}, nil
}

func (t *Table) searchIndex(key string) (offset, limit int64) {
	if key < t.index[0].key || key > t.index[len(t.index)-1].key {
		t.Logger.Tracef("Offset not found for %v, out of range %v-%v\n", key, t.index[0].key, t.index[len(t.index)-1].key)

		return -1, -1
	}

	// binary search
	idx := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].key >= key
	})

	if t.index[idx].key == key {
		t.Logger.Tracef("Found exact offset=limit=%v for %v\n", offset, limit, key)

		return t.index[idx].offset, t.index[idx].offset
	}

	t.Logger.Tracef("Found range offset=%v, limit=%v for %v\n", offset, limit, key)

	return t.index[idx-1].offset, t.index[idx].offset
}

func (t *Table) scanFile(reader *csv.Reader, key string, limit int64) ([]string, error) {
	var scannedLines int64

	for {
		cols, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("csv.Reader.Read: %w", err)
		}

		if cols[0] == key {
			// return copy due to ReuseRecord
			dup := make([]string, len(cols)-1)
			copy(dup, cols[1:])

			return dup, nil
		}

		scannedLines++

		if reader.InputOffset() >= limit {
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

type BulkResult struct {
	Values [][]string
}

var ErrKeysNotSorted = errors.New("keys are not sorted")

func (t *Table) BulkGet(keys []string) (BulkResult, error) {
	if len(keys) == 0 {
		return BulkResult{}, errors.New("no keys")
	}

	if len(keys) == 1 {
		res, err := t.Get(keys[0])
		if err != nil {
			return BulkResult{}, fmt.Errorf("Get: %w", err)
		}

		return BulkResult{[][]string{res.Values}}, nil
	}

	if !sort.StringsAreSorted(keys) {
		return BulkResult{}, ErrKeysNotSorted
	}

	chunks := t.bulkSearchIndices(keys)
	if chunks == nil {
		// all keys are out of range
		return BulkResult{make([][]string, len(keys))}, nil
	}

	file, err := os.Open(t.file)
	if err != nil {
		return BulkResult{}, fmt.Errorf("os.Open(%q): %w", t.file, err)
	}
	defer file.Close()

	values := make([][]string, 0, len(keys))

	for _, chunk := range chunks {
		_, err = file.Seek(chunk.offset, 0)
		if err != nil {
			return BulkResult{}, fmt.Errorf("file.Seek: %w", err)
		}

		reader := csv.NewReader(file)
		reader.ReuseRecord = true

		for _, key := range chunk.keys {
			value, err := t.scanFile(reader, key, chunk.limit-chunk.offset)
			if err != nil {
				return BulkResult{}, fmt.Errorf("scanFile: %w", err)
			}

			values = append(values, value)
		}
	}

	return BulkResult{values}, nil
}

type bulkSearchChunk struct {
	keys   []string
	offset int64
	limit  int64
}

// keys must be sorted and more than 2.
func (t *Table) bulkSearchIndices(keys []string) []*bulkSearchChunk {
	if keys[len(keys)-1] < t.index[0].key || t.index[len(t.index)-1].key < keys[0] {
		// all keys are out of range
		return nil
	}

	offset, limit := t.searchIndex(keys[0])

	lastChunk := &bulkSearchChunk{keys: []string{keys[0]}, offset: offset, limit: limit}
	chunks := []*bulkSearchChunk{lastChunk}

	for _, key := range keys[1:] {
		offset, limit := t.searchIndex(key)

		if offset == lastChunk.offset {
			lastChunk.keys = append(lastChunk.keys, key)

			continue
		}

		lastChunk = &bulkSearchChunk{keys: []string{key}, offset: offset, limit: limit}
		chunks = append(chunks, lastChunk)
	}

	return chunks
}
