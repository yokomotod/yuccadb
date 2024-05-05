package yuccadb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb"
	"github.com/yokomotod/yuccadb/internals/testdata"
)

// const (
// 	testDataDir   = "./testdata"
// 	testTableName = "test"
// )

// var tableSize int

// func init() {
// 	flag.IntVar(&tableSize, "table-size", 1_000_000, "size of the table")
// }

// func TestMain(m *testing.M) {
// 	flag.Parse()

// 	err := testdata.GenTestCsv(testDataDir, tableSize)
// 	if err != nil {
// 		log.Println(err)
// 		os.Exit(1)
// 	}

// 	// run test
// 	code := m.Run()

// 	os.Exit(code)
// }

func TestDB(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	tableSize := 10_000
	testFile, err := testdata.GenTestCsv(tempDir, tableSize)
	if err != nil {
		t.Fatalf("GenTestCsv: %v", err)
	}

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		t.Fatalf("db.PutTable: %v", err)
	}

	cases := []struct {
		name          string
		db            *yuccadb.YuccaDB
		key           string
		want          []string
		wantKeyExists bool
	}{
		{"key exists on index", db, "0000000000", []string{"0"}, true},
		{"key does not exist on index", db, "0000000999", []string{"999"}, true},
		{"last key", db, fmt.Sprintf("%010d", tableSize-1), []string{strconv.Itoa(tableSize - 1)}, true},
		{"before last key", db, fmt.Sprintf("%010d", tableSize-2), []string{strconv.Itoa(tableSize - 2)}, true},
		{"not found but middle of keys", db, "0000000999x", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			testDBCase(t, c.db, "test", c.key, c.want, c.wantKeyExists)
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

	expectedErr := "table \"test\" already exists and replace is false"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %s, but got %s", expectedErr, err.Error())
	}
}

func TestReplaceTable(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	testFile1 := filepath.Join(tempDir, "test_a.csv")
	testFile2 := filepath.Join(tempDir, "test_b.csv")

	db := yuccadb.NewYuccaDB()

	content := "key,value"
	if err := os.WriteFile(testFile1, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable("test", testFile1, false); err != nil {
		t.Fatal(err)
	}

	res, err := db.GetValue("test", "key")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res.Values, []string{"value"}) {
		t.Fatalf("expected value, but got %s", res.Values)
	}

	// replace
	content = "key,value2"
	if err := os.WriteFile(testFile2, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := db.PutTable("test", testFile2, true); err != nil {
		t.Fatal(err)
	}

	res, err = db.GetValue("test", "key")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(res.Values, []string{"value2"}) {
		t.Fatalf("expected value, but got %s", res.Values)
	}
}
