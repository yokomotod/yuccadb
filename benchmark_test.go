package yuccadb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yokomotod/yuccadb"
	"github.com/yokomotod/yuccadb/sstable"
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

func BenchmarkDB(b *testing.B) {
	ctx := context.Background()
	// dataDir, testTableName, testFile := "./testdata", "test", testFileName()
	dataDir, testTableName, _ := "./testdata", "test", testFileName()

	// // (re-)create data dir
	// if err := os.RemoveAll(dataDir); err != nil {
	// 	b.Fatal(err)
	// }
	// if err := os.MkdirAll(dataDir, 0755); err != nil {
	// 	b.Fatal(err)
	// }

	db, err := yuccadb.NewYuccaDB(ctx, dataDir)
	if err != nil {
		b.Fatal(err)
	}
	// if err := db.PutTable(ctx, testTableName, testFile, false); err != nil {
	// 	b.Fatal(err)
	// }

	b.ResetTimer()
	total := sstable.Profile{}

	t1 := time.Now()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%010d", i)

		res, err := db.GetValue(testTableName, key)
		if err != nil {
			b.Fatal(err)
		}
		if !res.TableExists {
			b.Fatalf("table %s does not exist", testTableName)
		}

		if !res.KeyExists {
			b.Fatalf("key %s does not exist", key)
		}

		total.SearchOffset += res.Profile.SearchOffset
		total.Open += res.Profile.Open
		total.Seek += res.Profile.Seek
		total.Scan += res.Profile.Scan

		i++
	}
	fmt.Printf("N: %d, time: %v, total: %+v\n", b.N, time.Since(t1), total)
}

func BenchmarkDBParallel(b *testing.B) {
	ctx := context.Background()
	dataDir, testTableName, testFile := "./testdata", "test", testFileName()

	// (re-)create data dir
	if err := os.RemoveAll(dataDir); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		b.Fatal(err)
	}

	db, err := yuccadb.NewYuccaDB(ctx, dataDir)
	if err != nil {
		b.Fatal(err)
	}
	if err := db.PutTable(ctx, testTableName, testFile, false); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("%010d", i)

			res, err := db.GetValue(testTableName, key)
			if err != nil {
				b.Fatal(err)
			}
			if !res.TableExists {
				b.Fatalf("table %s does not exist", testTableName)
			}

			if !res.KeyExists {
				b.Fatalf("key %s does not exist", key)
			}

			i++
		}
	})
}
