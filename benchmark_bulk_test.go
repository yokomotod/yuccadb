package yuccadb_test

import (
	"fmt"
	"testing"

	"github.com/yokomotod/yuccadb"
	"github.com/yokomotod/yuccadb/internals/testdata"
)

const bulkGetSize = 50

func BenchmarkDBBulk(b *testing.B) {
	testFile := testdata.TestCsvPath("./testdata", tableSize)

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		keySeed := 0

		for pb.Next() {
			keys := make([]string, bulkGetSize)

			for i := range bulkGetSize {
				key := fmt.Sprintf("%010d", keySeed+i*100)
				keys[i] = key
			}

			res, err := db.BulkGetValues("test", keys)
			if err != nil {
				b.Fatal(err)
			}

			if res.Values == nil {
				b.Fatalf("invalid result: keys %q", keys)
			}

			keySeed++
		}
	})
}

func BenchmarkDBNoBulk(b *testing.B) {
	testFile := testdata.TestCsvPath("./testdata", tableSize)

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		keySeed := 0

		for pb.Next() {
			for i := range bulkGetSize {
				key := fmt.Sprintf("%010d", keySeed+i*100)

				res, err := db.GetValue("test", key)
				if err != nil {
					b.Fatal(err)
				}

				if res.Values == nil {
					b.Fatalf("key %q does not exist", key)
				}
			}

			keySeed++
		}
	})
}
