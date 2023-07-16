package sstable_test

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yokomotod/yuccadb/sstable"
)

func TestLoadError(t *testing.T) {
	ctx := context.Background()

	tempDir := t.TempDir()
	tempDataDir := t.TempDir()

	lines := []string{
		"key\tvalue",
		"broken",
	}
	content := strings.Join(lines, "\n")
	brokenFile := filepath.Join(tempDir, "broken.tsv")

	if err := ioutil.WriteFile(brokenFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := sstable.NewSSTable(ctx, "broken", brokenFile, tempDataDir)
	if err == nil {
		t.Fatal("expected error")
	}
	expectedErr := " invalid line:"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Fatalf("expected %q to include %q", err.Error(), expectedErr)
	}

	// check tempDataDir is empty
	files, err := ioutil.ReadDir(tempDataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		filenames := make([]string, len(files))
		for i, f := range files {
			filenames[i] = f.Name()
		}
		t.Fatalf("expected 0 files, but found %s", filenames)
	}
}
