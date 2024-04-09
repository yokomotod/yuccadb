package yuccadb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb"
)

const (
	dataDir       = "./testdata"
	testTableName = "test"
)

func TestMain(m *testing.M) {
	err := genTestCsv(testFileName())
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// run test
	code := m.Run()

	os.Exit(code)
}

const tableSize = 1_000_000

func testFileName() string {
	size, unit := tableSize, ""
	units := []string{"k", "m", "g", "t"}

	for i := 0; size >= 1_000; size, i = size/1_000, i+1 {
		unit = units[i]
	}

	return fmt.Sprintf("./testfile/test%d%s.csv", size, unit)
}

func genTestCsv(testFile string) error {
	// check test file exists and skip generating
	if _, err := os.Stat(testFile); err == nil {
		log.Printf("Skip generating %s\n", testFile)

		return nil
	}

	log.Printf("Generating %s...\n", testFile)

	file, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	for i := range tableSize {
		key := fmt.Sprintf("%010d", i)
		value := strconv.Itoa(i)

		_, err := file.WriteString(key + "," + value + "\n")
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
	}

	return nil
}

func TestDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testFile := testFileName()

	// (re-)create data dir
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(ctx, testTableName, testFile, false); err != nil {
		t.Fatal(err)
	}

	db2, err := yuccadb.NewYuccaDB(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name          string
		db            *yuccadb.YuccaDB
		key           string
		want          string
		wantKeyExists bool
	}{
		{"key exists on index", db, "0000000000", "0", true},
		{"key does not exist on index", db, "0000099999", "99999", true},
		{"last key", db, fmt.Sprintf("%010d", tableSize-1), strconv.Itoa(tableSize - 1), true},
		{"before last key", db, fmt.Sprintf("%010d", tableSize-2), strconv.Itoa(tableSize - 2), true},
		{"not found but middle of keys", db, "0000099999x", "", false},
		{"key exists on reloaded index", db2, "0000000000", "0", true},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			testDBCase(t, c.db, testTableName, c.key, c.want, c.wantKeyExists)
		})
	}
}

func testDBCase(t *testing.T, db *yuccadb.YuccaDB, tableName, key, want string, wantKeyExists bool) {
	t.Helper()

	res, err := db.GetValue(tableName, key)
	if err != nil {
		t.Fatal(err)
	}

	if !res.TableExists {
		t.Fatalf("table %s does not exist", tableName)
	}

	if res.KeyExists != wantKeyExists {
		t.Fatalf("expected keyExists %t, but got %t", wantKeyExists, res.KeyExists)
	}

	if res.Value != want {
		t.Fatalf("expected %s, but got %s", want, res.Value)
	}
}

func TestLoadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	lines := []string{
		"key,value",
		"broken",
	}
	content := strings.Join(lines, "\n")
	brokenFile := filepath.Join(tempDir, "broken.csv")

	if err := os.WriteFile(brokenFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(tempDataDir)
	if err != nil {
		t.Fatal(err)
	}

	err = db.PutTable(ctx, "broken", brokenFile, false)
	if err == nil {
		t.Fatal("expected error")
	}

	expectedErr := " invalid line:"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Fatalf("expected %q to include %q", err.Error(), expectedErr)
	}

	// check tempDataDir is empty
	files, err := os.ReadDir(tempDataDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 0 {
		filenames := make([]string, len(files))
		for i, f := range files {
			filenames[i] = f.Name()
		}

		t.Fatalf("expected 0 files, but found %s", filenames)
	}
}

func TestDuplicateTableError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	content := "key,value"
	testFile := filepath.Join(tempDir, "test.csv")

	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(tempDataDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(ctx, "test", testFile, false); err != nil {
		t.Fatal(err)
	}

	err = db.PutTable(ctx, "test", testFile, false)
	if err == nil {
		t.Fatal("expected error")
	}

	expectedErr := "table test already exists and replace is false"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestReplaceTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tableName := "test"
	tempDir := t.TempDir()
	tempDataDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.csv")

	db, err := yuccadb.NewYuccaDB(tempDataDir)
	if err != nil {
		t.Fatal(err)
	}

	content := "key,value"
	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(ctx, tableName, testFile, false); err != nil {
		t.Fatal(err)
	}

	res, err := db.GetValue(tableName, "key")
	if err != nil {
		t.Fatal(err)
	}

	if res.Value != "value" {
		t.Fatalf("expected value, but got %s", res.Value)
	}

	// replace
	content = "key,value2"
	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(ctx, tableName, testFile, true); err != nil {
		t.Fatal(err)
	}

	res, err = db.GetValue(tableName, "key")
	if err != nil {
		t.Fatal(err)
	}

	if res.Value != "value2" {
		t.Fatalf("expected value, but got %s", res.Value)
	}
}
