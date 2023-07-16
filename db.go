package yuccadb

import (
	"context"
	"fmt"

	"github.com/yokomotod/yuccadb/sstable"
)

type YuccaDB struct {
	dataDir string
	tables  map[string]*sstable.SSTable
}

func NewYuccaDB() *YuccaDB {
	return &YuccaDB{
		dataDir: "./data",
		tables:  make(map[string]*sstable.SSTable),
	}
}

func (db *YuccaDB) PutTable(ctx context.Context, tableName, file string) error {
	table, err := sstable.NewSSTable(ctx, tableName, file, db.dataDir)
	if err != nil {
		return fmt.Errorf("failed to create table: %s", err)
	}

	db.tables[tableName] = table

	return nil
}

func (db *YuccaDB) GetValue(tableName, key string) (value string, tableExists, keyExists bool, err error) {
	ssTable, tableExists := db.tables[tableName]
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
