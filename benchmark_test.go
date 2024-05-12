package yuccadb_test

import (
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/yokomotod/yuccadb"
	"github.com/yokomotod/yuccadb/internals/testdata"
)

// const size = 10_000_000
// indexInterval: 1_000,
//
// $ go test -v -bench=. -run BenchmarkDB
// BenchmarkDB-8           	   30285	     37844 ns/op
// BenchmarkDBParallel-8   	   67245	     19990 ns/op

// const size = 100_000_000
// indexInterval: 1_000,
//
// $ go test -v -bench=. -run BenchmarkDB
// BenchmarkDB-8           	   37995	     39326 ns/op
// BenchmarkDBParallel-8   	   70704	     20128 ns/op

const tableSize = 1_000_000

func TestMain(m *testing.M) {
	if flag.Lookup("test.bench") != nil {
		_, err := testdata.GenTestCsv("./testdata", tableSize)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	}

	code := m.Run()
	os.Exit(code)
}

func BenchmarkDB(b *testing.B) {
	testFile := testdata.TestCsvPath("./testdata", tableSize)

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for keySeed := 0; keySeed < b.N; keySeed++ {
		key := fmt.Sprintf("%010d", keySeed)

		res, err := db.GetValue("test", key)
		if err != nil {
			b.Fatal(err)
		}

		if res.Values == nil {
			b.Fatalf("key %q does not exist", key)
		}

		keySeed++
	}
}

func BenchmarkDBParallel(b *testing.B) {
	testFile := testdata.TestCsvPath("./testdata", tableSize)

	db := yuccadb.NewYuccaDB()

	if err := db.PutTable("test", testFile, false); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		keySeed := 0
		for pb.Next() {
			key := fmt.Sprintf("%010d", keySeed)

			res, err := db.GetValue("test", key)
			if err != nil {
				b.Fatal(err)
			}

			if res.Values == nil {
				b.Fatalf("key %q does not exist", key)
			}

			keySeed++
		}
	})
}
