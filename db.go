package yuccadb

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/yokomotod/yuccadb/sstable"
)

type YuccaDB struct {
	dataDir string
	tables  map[string]*sstable.SSTable
}

func NewYuccaDB(ctx context.Context, dataDir string) (*YuccaDB, error) {
	db := &YuccaDB{
		dataDir: dataDir,
		tables:  make(map[string]*sstable.SSTable),
	}

	if err := db.loadExistingTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to load existing tables: %s", err)
	}

	return db, nil
}

func (db *YuccaDB) loadExistingTables(ctx context.Context) error {
	files, err := os.ReadDir(db.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %s", err)
	}

	for _, file := range files {
		tableName := strings.Split(file.Name(), ".")[0]
		filePath := fmt.Sprintf("%s/%s", db.dataDir, file.Name())

		fmt.Printf("loading table %s from %s\n", tableName, filePath)
		table, err := sstable.NewSSTable(ctx, tableName, filePath, db.dataDir)
		if err != nil {
			return fmt.Errorf("failed to load table: %s", err)
		}

		db.tables[tableName] = table
	}

	return nil
}

func (db *YuccaDB) CreateTable(ctx context.Context, tableName, file string) error {
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
