package testdata

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func toHumanReadable(n int) string {
	unit := ""
	units := []string{"k", "m", "g", "t"}

	for i := 0; n >= 1_000; n, i = n/1_000, i+1 {
		unit = units[i]
	}

	return strconv.Itoa(n) + unit
}

func TestCsvPath(outputDir string, csvSize int) string {
	suffix := toHumanReadable(csvSize)
	return outputDir + "/test" + suffix + ".csv"
}

func GenTestCsv(outputDir string, csvSize int) (string, error) {
	testCsvPath := TestCsvPath(outputDir, csvSize)

	// check test file exists and skip generating
	if _, err := os.Stat(testCsvPath); err == nil {
		log.Printf("Skip generating %q\n", testCsvPath)

		return testCsvPath, nil
	}

	log.Printf("Generating %q...\n", testCsvPath)

	if _, err := os.Stat(outputDir); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("os.Stat: %w", err)
		}
		if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
			return "", fmt.Errorf("os.MkdirAll: %w", err)
		}
	}

	file, err := os.OpenFile(testCsvPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("os.OpenFile: %w", err)
	}

	for i := range csvSize {
		key := fmt.Sprintf("%010d", i)
		value := strconv.Itoa(i)

		if _, err := file.WriteString(key + "," + value + "\n"); err != nil {
			return "", fmt.Errorf("WriteString: %w", err)
		}
	}

	return testCsvPath, nil
}
