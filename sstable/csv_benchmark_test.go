// compare csv.Reader vs bufio.Scanner performance
package sstable_test

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func BenchmarkScanner(b *testing.B) {
	file, err := os.Open("../testfile/test1m.csv")
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
	file, err := os.Open("../testfile/test1m.csv")
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
	file, err := os.Open("../testfile/test1m.csv")
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
