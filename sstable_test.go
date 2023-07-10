package yuccadb_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/yokomotod/yuccadb"
)

func TestMain(m *testing.M) {
	err := genTestTsv()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// run test
	code := m.Run()

	os.Exit(code)
}

const testFile = "test.tsv"

func genTestTsv() error {
	fmt.Printf("Generating %s...\n", testFile)

	f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %s", err)
	}

	for i := 0; i < 10_000_000; i++ {
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
