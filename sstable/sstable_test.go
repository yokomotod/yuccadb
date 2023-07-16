package sstable_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/yokomotod/yuccadb/sstable"
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
	return fmt.Sprintf("../testfile/test%d%s.tsv", s, unit)

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

func TestSSTable(t *testing.T) {
	ctx := context.Background()
	dataDir, testTableName, testFile := "../testdata", "test", testFileName()

	ssTable, err := sstable.NewSSTable(ctx, testTableName, testFile, dataDir)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name          string
		key           string
		want          string
		wantKeyExists bool
	}{
		{"key exists on index", "0000000000", "0", true},
		{"key does not exist on index", "0000099999", "99999", true},
		{"not found but middle of keys", "0000099999x", "", false},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {

			got, keyExists, err := ssTable.Get(c.key)
			if err != nil {
				t.Fatal(err)
			}

			if keyExists != c.wantKeyExists {
				t.Fatalf("expected keyExists %t, but got %t", c.wantKeyExists, keyExists)
			}
			if got != c.want {
				t.Fatalf("expected %s, but got %s", c.want, got)
			}
		})
	}
}
