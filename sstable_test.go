package yuccadb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/yokomotod/yuccadb"
)

func TestReadSSTable(t *testing.T) {
	testFile := "test.tsv"

	f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%010d", i)
		value := fmt.Sprint(i)

		_, err := f.WriteString(key + "\t" + value + "\n")
		if err != nil {
			t.Fatal(err)
		}
	}

	ssTable := yuccadb.NewSsTable(testFile)

	value, err := ssTable.Read("0000000042")
	if err != nil {
		t.Fatal(err)
	}

	if value != "42" {
		t.Fatalf("expected 42, but got %s", value)
	}
}
