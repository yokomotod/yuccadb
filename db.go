package yuccadb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yokomotod/yuccadb/logger"
	"github.com/yokomotod/yuccadb/table"
)

type yuccaTable struct {
	table     *table.Table
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

func (db *YuccaDB) TableTimestamp(tableName string) (time.Time, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.tables[tableName]
	if !ok {
		return time.Time{}, false
	}

	return table.timestamp, true
}

func (db *YuccaDB) validatePutTable(tableName, file string, replace bool) error {
	if _, ok := db.tables[tableName]; ok && !replace {
		return fmt.Errorf("table %q already exists and replace is false", tableName)
	}

	for _, table := range db.tables {
		if table.table.File == file {
			return fmt.Errorf("file %q is already used by table %q", file, tableName)
		}
	}

	return nil
}

func (db *YuccaDB) PutTable(tableName, file string, replace bool) error {
	db.mu.RLock()
	// pre-validate before heavy BuildTable process
	err := db.validatePutTable(tableName, file, replace)
	db.mu.RUnlock()

	if err != nil {
		return err
	}

	table, err := table.BuildTable(file, db.Logger)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	db.mu.Lock()
	// re-validate with lock
	if err := db.validatePutTable(tableName, file, replace); err != nil {
		db.mu.Unlock()

		return err
	}

	oldTable, hadOldTable := db.tables[tableName]
	db.tables[tableName] = yuccaTable{
		table:     table,
		timestamp: time.Now(),
	}
	db.mu.Unlock()

	if !hadOldTable {
		return nil
	}

	db.Logger.Debugf("Remove old table file: %q\n", oldTable.table.File)

	if err = os.Remove(oldTable.table.File); err != nil {
		return fmt.Errorf("failed to remove old table file: %w", err)
	}

	return nil
}

type Result struct {
	Values  []string
	Profile table.Profile
}

var ErrTableNotFound = errors.New("table not found")

func (db *YuccaDB) GetValue(tableName, key string) (Result, error) {
	db.mu.RLock()
	table, tableExists := db.tables[tableName]
	db.mu.RUnlock()

	if !tableExists {
		return Result{}, ErrTableNotFound
	}

	res, err := table.table.Get(key)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get value: %w", err)
	}

	return Result{
		Values:  res.Values,
		Profile: res.Profile,
	}, nil
}
