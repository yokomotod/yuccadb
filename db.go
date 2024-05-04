package yuccadb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/yokomotod/yuccadb/sstable"
)

type YuccaDB struct {
	tables map[string]*sstable.SSTable
	mu     sync.RWMutex
}

func NewYuccaDB() *YuccaDB {
	db := &YuccaDB{
		tables: make(map[string]*sstable.SSTable),
	}

	return db
}

func (db *YuccaDB) HasTable(tableName string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, ok := db.tables[tableName]

	return ok
}

func (db *YuccaDB) Tables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.tables))
	for table := range db.tables {
		tables = append(tables, table)
	}

	return tables
}

func (db *YuccaDB) PutTable(tableName, file string, replace bool) error {
	if db.HasTable(tableName) && !replace {
		return fmt.Errorf("table %s already exists and replace is false", tableName)
	}

	table, err := sstable.NewSSTable(file)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	db.mu.Lock()
	db.tables[tableName] = table
	db.mu.Unlock()

	return nil
}

type Result struct {
	Values  []string
	Profile sstable.Profile
}

var ErrTableNotFound = errors.New("table not found")

func (db *YuccaDB) GetValue(tableName, key string) (Result, error) {
	db.mu.RLock()
	ssTable, tableExists := db.tables[tableName]
	db.mu.RUnlock()

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
