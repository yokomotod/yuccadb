package yuccadb

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
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

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %s", err)
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
		table, err := sstable.NewSSTable(ctx, tableName, filePath)
		if err != nil {
			return fmt.Errorf("failed to load table: %s", err)
		}

		db.tables[tableName] = table
	}

	return nil
}

func (db *YuccaDB) PutTable(ctx context.Context, tableName, file string) error {
	if _, ok := db.tables[tableName]; ok {
		return fmt.Errorf("table %s already exists", tableName)
	}

	localFile := fmt.Sprintf("%s/%s.tsv", db.dataDir, tableName)
	tmpFile := fmt.Sprintf("%s.tmp", localFile)
	if localFile == file {
		tmpFile = localFile
	} else {
		prepareFile(ctx, file, tmpFile)
	}

	table, err := sstable.NewSSTable(ctx, tableName, tmpFile)
	if err != nil {
		if tmpFile != localFile {
			_ = os.Remove(tmpFile)
		}
		return fmt.Errorf("failed to create table: %s", err)
	}

	if tmpFile != localFile {
		if _, err := os.Stat(localFile); err == nil {
			return fmt.Errorf("file already exists: %s", localFile)
		} else if !os.IsNotExist(err) {
			return err
		}
		err = os.Rename(tmpFile, localFile)
		if err != nil {
			return fmt.Errorf("failed to rename file: %s", err)
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
			return fmt.Errorf("failed to download file: %s", err)
		}
	} else {
		err := copy(tmpFile, srcFile)
		if err != nil {
			return fmt.Errorf("failed to copy file: %s", err)
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
	f, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}
	defer f.Close()

	pathSegments := strings.Split(gcsPath, "/")
	bucket := pathSegments[2]
	object := strings.Join(pathSegments[3:], "/")

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	fmt.Printf("Downloading %v from %v\n", object, bucket)
	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	fmt.Printf("Downloaded %v\n", object)

	return nil
}

func copy(localFile, srcPath string) error {
	if localFile == srcPath {
		fmt.Printf("Skip copying %v\n", srcPath)
		return nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}
	defer src.Close()

	// use OpenFile with os.O_EXCL instead of Create to avoid overwriting
	dst, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("failed to create file: %s", err)
	}
	defer dst.Close()

	fmt.Printf("Copying %v to %v\n", srcPath, localFile)
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to copy file: %s", err)
	}

	fmt.Printf("Copied %v\n", srcPath)

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
