package yuccadb

import (
	"context"
	"fmt"

	"github.com/yokomotod/yuccadb/sstable"
)

type YuccaDB struct {
	tables map[string]*sstable.SSTable
}

func NewYuccaDB() *YuccaDB {
	return &YuccaDB{
		tables: make(map[string]*sstable.SSTable),
	}
}

func (s *YuccaDB) PutTable(ctx context.Context, tableName, file string) error {
	table, err := sstable.NewSSTable(ctx, file)
	if err != nil {
		return fmt.Errorf("failed to create table: %s", err)
	}

	s.tables[tableName] = table

	return nil
}

func (s *YuccaDB) GetValue(tableName, key string) (value string, tableExists, keyExists bool, err error) {
	ssTable, tableExists := s.tables[tableName]
	if !tableExists {
		return "", false, false, nil
	}

	value, keyExists, err = ssTable.Get(key)
	if err != nil {
		return "", true, false, fmt.Errorf("failed to get value: %s", err)
	}
	if !keyExists {
		return "", true, false, nil
	}

	return value, true, true, nil
}
