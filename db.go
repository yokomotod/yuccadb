package yuccadb

import (
	"errors"
	"fmt"

	"github.com/yokomotod/yuccadb/sstable"
)

type YuccaDB struct {
	tables map[string]*sstable.SSTable
}

func NewYuccaDB() *YuccaDB {
	db := &YuccaDB{
		tables: make(map[string]*sstable.SSTable),
	}

	return db
}

func (db *YuccaDB) HasTable(tableName string) bool {
	_, ok := db.tables[tableName]

	return ok
}

func (db *YuccaDB) Tables() []string {
	tables := make([]string, 0, len(db.tables))
	for table := range db.tables {
		tables = append(tables, table)
	}

	return tables
}

func (db *YuccaDB) PutTable(tableName, file string, replace bool) error {
	if _, ok := db.tables[tableName]; ok && !replace {
		return fmt.Errorf("table %s already exists and replace is false", tableName)
	}

	table, err := sstable.NewSSTable(file)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	db.tables[tableName] = table

	return nil
}

type Result struct {
	Values  []string
	Profile sstable.Profile
}

var ErrTableNotFound = errors.New("table not found")

func (db *YuccaDB) GetValue(tableName, key string) (Result, error) {
	ssTable, tableExists := db.tables[tableName]
	if !tableExists {
		return Result{}, ErrTableNotFound
	}

	res, err := ssTable.Get(key)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get value: %w", err)
	}

	return Result{
		Values:  res.Values,
		Profile: res.Profile,
	}, nil
}
