package yuccadb_test

import (
	"context"
	"fmt"
	"os"
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
	return fmt.Sprintf("testdata/test%d%s.tsv", s, unit)

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
	testFile := testFileName()
	ssTable, err := yuccadb.NewSSTable(ctx, testFile)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		key  string
		want string
	}{
		{"key exists on index", "0000000000", "0"},
		{"key does not exist on index", "0000099999", "99999"},
		{"not found but middle of keys", "0000099999x", ""},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {

			got, err := ssTable.Get(c.key)
			if err != nil {
				t.Fatal(err)
			}

			if got != c.want {
				t.Fatalf("expected %s, but got %s", c.want, got)
			}
		})
	}
}
