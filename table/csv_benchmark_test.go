// compare csv.Reader vs bufio.Scanner performance
package table_test

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb/internals/testdata"
)

var tableSize int

func init() {
	flag.IntVar(&tableSize, "size", 1_000_000, "size of the table")
}

func TestMain(m *testing.M) {
	flag.Parse()

	if flag.Lookup("test.bench") != nil {
		_, err := testdata.GenTestCsv("../testdata", tableSize)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	}

	code := m.Run()
	os.Exit(code)
}

func BenchmarkScanner(b *testing.B) {
	testFilePath := testdata.TestCsvPath("../testdata", tableSize)

	file, err := os.Open(testFilePath)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	b.ResetTimer()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		_ = strings.Split(line, ",")
	}
}

func BenchmarkCSVReader(b *testing.B) {
	testFilePath := testdata.TestCsvPath("../testdata", tableSize)

	file, err := os.Open(testFilePath)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	b.ResetTimer()

	reader := csv.NewReader(file)

	for {
		_, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.Fatal(err)
		}
	}
}

func BenchmarkCSVReaderWithReuseRecord(b *testing.B) {
	testFilePath := testdata.TestCsvPath("../testdata", tableSize)

	file, err := os.Open(testFilePath)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	b.ResetTimer()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	for {
		_, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.Fatal(err)
		}
	}
}
