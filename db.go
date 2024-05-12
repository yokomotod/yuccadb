package yuccadb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yokomotod/yuccadb/logger"
	yuccaTable "github.com/yokomotod/yuccadb/table"
)

type YuccaDB struct {
	tables map[string]*yuccaTable.Table
	mu     sync.RWMutex
	Logger logger.Logger
}

func NewYuccaDB() *YuccaDB {
	db := &YuccaDB{
		tables: make(map[string]*yuccaTable.Table),
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

	return table.Timestamp(), true
}

func (db *YuccaDB) validatePutTable(tableName, file string, replace bool) error {
	if _, ok := db.tables[tableName]; ok && !replace {
		return fmt.Errorf("table %q already exists and replace is false", tableName)
	}

	for _, table := range db.tables {
		if table.File() == file {
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

	table, err := yuccaTable.BuildTable(file, db.Logger)
	if err != nil {
		return fmt.Errorf("table.BuildTable: %w", err)
	}

	db.mu.Lock()
	// re-validate with lock
	if err := db.validatePutTable(tableName, file, replace); err != nil {
		db.mu.Unlock()

		return err
	}

	oldTable, hadOldTable := db.tables[tableName]
	db.tables[tableName] = table
	db.mu.Unlock()

	if !hadOldTable {
		return nil
	}

	db.Logger.Debugf("Remove old table file: %q\n", oldTable.File())

	if err = os.Remove(oldTable.File()); err != nil {
		return fmt.Errorf("os.Remove(%q): %w", oldTable.File(), err)
	}

	return nil
}

var ErrTableNotFound = errors.New("table not found")

func (db *YuccaDB) GetValue(tableName, key string) (yuccaTable.Result, error) {
	db.mu.RLock()
	table, tableExists := db.tables[tableName]
	db.mu.RUnlock()

	if !tableExists {
		return yuccaTable.Result{}, ErrTableNotFound
	}

	res, err := table.Get(key)
	if err != nil {
		return yuccaTable.Result{}, fmt.Errorf("table.Get: %w", err)
	}

	return res, nil
}

func (db *YuccaDB) BulkGetValues(tableName string, keys []string) (yuccaTable.BulkResult, error) {
	db.mu.RLock()
	table, tableExists := db.tables[tableName]
	db.mu.RUnlock()

	if !tableExists {
		return yuccaTable.BulkResult{}, ErrTableNotFound
	}

	res, err := table.BulkGet(keys)
	if err != nil {
		return yuccaTable.BulkResult{}, fmt.Errorf("table.Get: %w", err)
	}

	return res, nil
}
