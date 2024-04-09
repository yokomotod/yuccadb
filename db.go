package yuccadb

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/yokomotod/yuccadb/sstable"
)

const (
	dataDirPermision  = 0o755
	dataFilePermision = 0o644
)

type YuccaDB struct {
	dataDir string
	tables  map[string]*sstable.SSTable
}

func NewYuccaDB(dataDir string) (*YuccaDB, error) {
	db := &YuccaDB{
		dataDir: dataDir,
		tables:  make(map[string]*sstable.SSTable),
	}

	if err := os.MkdirAll(dataDir, dataDirPermision); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := db.loadExistingTables(); err != nil {
		return nil, fmt.Errorf("failed to load existing tables: %w", err)
	}

	return db, nil
}

func (db *YuccaDB) loadExistingTables() error {
	files, err := os.ReadDir(db.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, file := range files {
		tableName := strings.Split(file.Name(), ".")[0]
		filePath := fmt.Sprintf("%s/%s", db.dataDir, file.Name())

		log.Printf("loading table %s from %s\n", tableName, filePath)

		table, err := sstable.NewSSTable(filePath)
		if err != nil {
			return fmt.Errorf("failed to load table: %w", err)
		}

		db.tables[tableName] = table
	}

	return nil
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

func (db *YuccaDB) PutTable(ctx context.Context, tableName, file string, replace bool) error {
	if _, ok := db.tables[tableName]; ok && !replace {
		return fmt.Errorf("table %s already exists and replace is false", tableName)
	}

	localFile := db.dataDir + "/" + tableName + ".csv"
	tmpFile := localFile + ".tmp"

	if localFile == file {
		tmpFile = localFile
	} else {
		err := prepareFile(ctx, file, tmpFile)
		if err != nil {
			return fmt.Errorf("failed to prepare file: %w", err)
		}
	}

	table, err := sstable.NewSSTable(tmpFile)
	if err != nil {
		if tmpFile != localFile {
			_ = os.Remove(tmpFile)
		}

		return fmt.Errorf("failed to create table: %w", err)
	}

	if tmpFile != localFile {
		err = os.Rename(tmpFile, localFile)
		if err != nil {
			return fmt.Errorf("failed to rename file: %w", err)
		}

		table.File = localFile
	}

	db.tables[tableName] = table

	return nil
}

func prepareFile(ctx context.Context, srcFile, tmpFile string) error {
	if strings.HasPrefix(srcFile, "gs://") {
		err := download(ctx, tmpFile, srcFile)
		if err != nil {
			return fmt.Errorf("failed to download file: %w", err)
		}
	} else {
		err := cp(tmpFile, srcFile)
		if err != nil {
			return fmt.Errorf("failed to copy file: %w", err)
		}
	}

	return nil
}

func download(ctx context.Context, localFile, gcsPath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	// use OpenFile with os.O_EXCL instead of Create to avoid overwriting
	file, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, dataFilePermision)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}
	defer file.Close()

	pathSegments := strings.Split(gcsPath, "/")
	bucket := pathSegments[2]
	object := strings.Join(pathSegments[3:], "/")

	reader, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer reader.Close()

	log.Printf("Downloading %v from %v\n", object, bucket)

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}

	log.Printf("Downloaded %v\n", object)

	return nil
}

func cp(localFile, srcPath string) error {
	if localFile == srcPath {
		log.Printf("Skip copying %v\n", srcPath)

		return nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer src.Close()

	// use OpenFile with os.O_EXCL instead of Create to avoid overwriting
	dst, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, dataFilePermision)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer dst.Close()

	log.Printf("Copying %v to %v\n", srcPath, localFile)

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	log.Printf("Copied %v\n", srcPath)

	return nil
}

type Result struct {
	Value       string
	TableExists bool
	KeyExists   bool
	Profile     sstable.Profile
}

func (db *YuccaDB) GetValue(tableName, key string) (Result, error) {
	ssTable, tableExists := db.tables[tableName]
	if !tableExists {
		return Result{}, nil
	}

	res, err := ssTable.Get(key)
	if err != nil {
		return Result{}, fmt.Errorf("failed to get value: %w", err)
	}

	return Result{
		Value:       res.Value,
		TableExists: true,
		KeyExists:   res.KeyExists,
		Profile:     res.Profile,
	}, nil
}
