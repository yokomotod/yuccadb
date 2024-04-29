package yuccadb_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb"
)

const (
	testFileDir   = "./testfile"
	testTableName = "test"
)

func TestMain(m *testing.M) {
	err := genTestCsv()
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

	return fmt.Sprintf("./%s/test%d%s.csv", testFileDir, size, unit)
}

func genTestCsv() error {
	testFile := testFileName()

	// check test file exists and skip generating
	if _, err := os.Stat(testFile); err == nil {
		log.Printf("Skip generating %s\n", testFile)

		return nil
	}

	log.Printf("Generating %s...\n", testFile)

	if _, err := os.Stat(testFileDir); os.IsNotExist(err) {
		if err := os.Mkdir(testFileDir, 0o755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
	}

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

	testFile := testFileName()

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable(testTableName, testFile, false); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name          string
		db            *yuccadb.YuccaDB
		key           string
		want          []string
		wantKeyExists bool
	}{
		{"key exists on index", db, "0000000000", []string{"0"}, true},
		{"key does not exist on index", db, "0000099999", []string{"99999"}, true},
		{"last key", db, fmt.Sprintf("%010d", tableSize-1), []string{strconv.Itoa(tableSize - 1)}, true},
		{"before last key", db, fmt.Sprintf("%010d", tableSize-2), []string{strconv.Itoa(tableSize - 2)}, true},
		{"not found but middle of keys", db, "0000099999x", nil, false},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			testDBCase(t, c.db, testTableName, c.key, c.want, c.wantKeyExists)
		})
	}
}

func testDBCase(t *testing.T, db *yuccadb.YuccaDB, tableName, key string, want []string, wantKeyExists bool) {
	t.Helper()

	res, err := db.GetValue(tableName, key)
	if err != nil {
		t.Fatal(err)
	}

	if (res.Values != nil) != wantKeyExists {
		t.Fatalf("expected keyExists %t, but got %t", wantKeyExists, (res.Values != nil))
	}

	if !reflect.DeepEqual(res.Values, want) {
		t.Fatalf("expected %v, but got %v", want, res.Values)
	}
}

func TestLoadError(t *testing.T) {
	t.Skip("TODO")
	t.Parallel()

	tempDir := t.TempDir()

	lines := []string{
		"key,value",
		"broken",
	}
	content := strings.Join(lines, "\n")
	brokenFile := filepath.Join(tempDir, "broken.csv")

	if err := os.WriteFile(brokenFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	db := yuccadb.NewYuccaDB()

	err := db.PutTable("broken", brokenFile, false)
	if err == nil {
		t.Fatal("expected error")
	}

	expectedErr := " invalid line:"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Fatalf("expected %q to include %q", err.Error(), expectedErr)
	}
}

func TestDuplicateTableError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	content := "key,value"
	testFile := filepath.Join(tempDir, "test.csv")

	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		t.Fatal(err)
	}

	err := db.PutTable("test", testFile, false)
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

	tableName := "test"
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.csv")

	db := yuccadb.NewYuccaDB()

	content := "key,value"
	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(tableName, testFile, false); err != nil {
		t.Fatal(err)
	}

	res, err := db.GetValue(tableName, "key")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res.Values, []string{"value"}) {
		t.Fatalf("expected value, but got %s", res.Values)
	}

	// replace
	content = "key,value2"
	if err := os.WriteFile(testFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable(tableName, testFile, true); err != nil {
		t.Fatal(err)
	}

	res, err = db.GetValue(tableName, "key")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res.Values, []string{"value2"}) {
		t.Fatalf("expected value, but got %s", res.Values)
	}
}
