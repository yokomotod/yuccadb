package yuccadb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb"
)

func TestMain(m *testing.M) {
	err := genTestTsv(testFileName())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// run test
	code := m.Run()

	os.Exit(code)
}

const size = 1_000_000

func testFileName() string {
	s, unit := size, ""
	units := []string{"k", "m", "g", "t"}
	for i := 0; s >= 1_000; s, i = s/1_000, i+1 {
		unit = units[i]
	}
	return fmt.Sprintf("./testfile/test%d%s.tsv", s, unit)

}

func genTestTsv(testFile string) error {
	// check test file exists and skip generating
	if _, err := os.Stat(testFile); err == nil {
		fmt.Printf("Skip generating %s\n", testFile)
		return nil
	}

	fmt.Printf("Generating %s...\n", testFile)

	f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}

	for i := 0; i < size; i++ {
		key := fmt.Sprintf("%010d", i)
		value := fmt.Sprint(i)

		_, err := f.WriteString(key + "\t" + value + "\n")
		if err != nil {
			return fmt.Errorf("failed to write file: %s", err)
		}
	}

	return nil
}

func TestDB(t *testing.T) {
	ctx := context.Background()
	dataDir, testTableName, testFile := "./testdata", "test", testFileName()

	// (re-)create data dir
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PutTable(ctx, testTableName, testFile, false); err != nil {
		t.Fatal(err)
	}

	db2, err := yuccadb.NewYuccaDB(ctx, dataDir)
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
		{"not found but middle of keys", db, "0000099999x", "", false},
		{"key exists on reloaded index", db2, "0000000000", "0", true},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {

			res, err := c.db.GetValue(testTableName, c.key)
			if err != nil {
				t.Fatal(err)
			}
			if !res.TableExists {
				t.Fatalf("table %s does not exist", testTableName)
			}

			if res.KeyExists != c.wantKeyExists {
				t.Fatalf("expected keyExists %t, but got %t", c.wantKeyExists, res.KeyExists)
			}
			if res.Value != c.want {
				t.Fatalf("expected %s, but got %s", c.want, res.Value)
			}
		})
	}
}

func TestLoadError(t *testing.T) {
	ctx := context.Background()

	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	lines := []string{
		"key\tvalue",
		"broken",
	}
	content := strings.Join(lines, "\n")
	brokenFile := filepath.Join(tempDir, "broken.tsv")

	if err := os.WriteFile(brokenFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(ctx, tempDataDir)
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
	ctx := context.Background()

	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	content := "key\tvalue"
	testFile := filepath.Join(tempDir, "test.tsv")
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(ctx, tempDataDir)
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
	ctx := context.Background()
	tableName := "test"
	tempDir := t.TempDir()
	tempDataDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.tsv")

	db, err := yuccadb.NewYuccaDB(ctx, tempDataDir)
	if err != nil {
		t.Fatal(err)
	}

	content := "key\tvalue"
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
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
	content = "key\tvalue2"
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
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
