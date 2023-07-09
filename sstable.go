package yuccadb

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type ssTable struct {
	file          string
	index         *rbt.Tree
	indexInterval int
}

func NewSsTable(tsvFile string) *ssTable {
	t := &ssTable{
		file:          tsvFile,
		index:         rbt.NewWithStringComparator(),
		indexInterval: 10,
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

		if i%t.indexInterval == 0 {
			fmt.Printf("Offset: %d Line: %s\n", offset, line)
			t.index.Put(key, int64(offset))
		}

		offset += len(line) + 1
		i++
	}

	if scanner.Err() != nil {
		return fmt.Errorf("failed to scan file: %s", err)
	}

	return nil
}

func (t *ssTable) Read(key string) (string, error) {
	node, ok := t.index.Floor(key)

	if !ok {
		return "", nil
	}

	fmt.Printf("Found offset: %v for %v\n", node.Value, node.Key)

	// interface{} to int
	offset, ok := node.Value.(int64)
	if !ok {
		return "", fmt.Errorf("invalid offset: %v", node.Value)
	}

	// open file and seek to offset
	f, err := os.Open(t.file)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", err)
	}
	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek file: %s", err)
	}

	var value string

	// read line
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		fmt.Printf("Read: %s\n", line)

		// split line and return value
		cols := strings.Split(line, "\t")
		if len(cols) != 2 {
			return "", fmt.Errorf("invalid line: %s", line)
		}

		if cols[0] == key {
			value = cols[1]
			break
		}
	}

	if scanner.Err() != nil {
		return "", fmt.Errorf("failed to scan file: %s", err)
	}

	return value, nil
}
