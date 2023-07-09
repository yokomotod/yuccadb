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

	cases := []struct {
		name string
		key  string
		want string
	}{
		{"key exists on index", "0000000000", "0"},
		{"key does not exist on index", "0000000042", "42"},
	}

	for _, c := range cases {
		// sub test
		t.Run(c.name, func(t *testing.T) {

			got, err := ssTable.Read(c.key)
			if err != nil {
				t.Fatal(err)
			}

			if got != c.want {
				t.Fatalf("expected %s, but got %s", c.want, got)
			}
		})
	}
}
