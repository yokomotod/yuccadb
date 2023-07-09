package yuccadb

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type ssTable struct {
	file  string
	index *rbt.Tree
}

func NewSsTable(tsvFile string) *ssTable {
	t := &ssTable{
		file:  tsvFile,
		index: rbt.NewWithStringComparator(),
	}

	t.load(tsvFile)

	return t
}

func (t *ssTable) load(tsvFile string) error {
	f, err := os.Open(tsvFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	i, offset := 0, 0

	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, "\t")
		key := cols[0]

		fmt.Printf("Offset: %d Line: %s\n", offset, line)
		t.index.Put(key, int64(offset))

		offset += len(line) + 1
		i++
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %s", err)
	}

	return nil
}

func (t *ssTable) Read(key string) (string, error) {
	offset, ok := t.index.Get(key)

	if !ok {
		return "", nil
	}

	// interface{} to int
	offsetInt64, ok := offset.(int64)
	if !ok {
		return "", fmt.Errorf("invalid offset: %v", offset)
	}

	// open file and seek to offset
	f, err := os.Open(t.file)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	_, err = f.Seek(offsetInt64, 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek file: %s", err)
	}

	// read line
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	line := scanner.Text()

	if scanner.Err() != nil {
		return "", fmt.Errorf("failed to scan file: %s", err)
	}

	// split line and return value
	cols := strings.Split(line, "\t")
	if len(cols) != 2 {
		return "", fmt.Errorf("invalid line: %s", line)
	}

	// assert key
	if cols[0] != key {
		return "", fmt.Errorf("invalid key: %s", key)
	}

	return cols[1], nil
}
