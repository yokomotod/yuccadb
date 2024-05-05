package yuccadb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yokomotod/yuccadb/logger"
	"github.com/yokomotod/yuccadb/sstable"
)

type yuccaTable struct {
	ssTable   *sstable.SSTable
	timestamp time.Time
}

type YuccaDB struct {
	tables map[string]yuccaTable
	mu     sync.RWMutex
	Logger logger.Logger
}

func NewYuccaDB() *YuccaDB {
	db := &YuccaDB{
		tables: make(map[string]yuccaTable),
		Logger: &logger.DefaultLogger{
			Level: logger.Warning,
		},
	}

	return db
}

func (db *YuccaDB) HasTable(tableName string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, ok := db.tables[tableName]

	return ok
}

func (db *YuccaDB) TableTimestamp(tableName string) (time.Time, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.tables[tableName]
	if !ok {
		return time.Time{}, false
	}

	return table.timestamp, true
}

func (db *YuccaDB) PutTable(tableName, file string, replace bool) error {
	if db.HasTable(tableName) && !replace {
		return fmt.Errorf("table %q already exists and replace is false", tableName)
	}

	table, err := sstable.NewSSTable(file, db.Logger)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	db.mu.Lock()
	oldTable, hadOldTable := db.tables[tableName]
	db.tables[tableName] = yuccaTable{
		ssTable:   table,
		timestamp: time.Now(),
	}
	db.mu.Unlock()

	if !hadOldTable {
		return nil
	}

	db.Logger.Debugf("Remove old table file: %q\n", oldTable.ssTable.File)

	if err = os.Remove(oldTable.ssTable.File); err != nil {
		return fmt.Errorf("failed to remove old table file: %w", err)
	}

	return nil
}

type Result struct {
	Values  []string
	Profile sstable.Profile
}

var ErrTableNotFound = errors.New("table not found")

func (db *YuccaDB) GetValue(tableName, key string) (Result, error) {
	db.mu.RLock()
	table, tableExists := db.tables[tableName]
	db.mu.RUnlock()

	if !tableExists {
		return Result{}, ErrTableNotFound
	}

	res, err := table.ssTable.Get(key)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get value: %w", err)
	}

	return Result{
		Values:  res.Values,
		Profile: res.Profile,
	}, nil
}
