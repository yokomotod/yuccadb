package yuccadb

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type ssTable struct {
	file  string
	index map[string]int
}

func NewSsTable(tsvFile string) *ssTable {
	t := &ssTable{
		file:  tsvFile,
		index: make(map[string]int),
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
	var offset int = 0

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Printf("Offset: %d Line: %s\n", offset, line)

		cols := strings.Split(line, "\t")
		key := cols[0]

		t.index[key] = offset

		offset += len(line) + 1
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %s", err)
	}

	return nil
}

func (t *ssTable) Read(key string) (string, error) {
	offset, ok := t.index[key]

	if !ok {
		return "", nil
	}

	// open file and seek to offset
	f, err := os.Open(t.file)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	_, err = f.Seek(int64(offset), 0)
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
