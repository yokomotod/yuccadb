package humanize_test

import (
	"strconv"
	"testing"

	"github.com/yokomotod/yuccadb/internals/humanize"
)

func TestComma(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{-12, "-12"},
		{123, "123"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{-12345678, "-12,345,678"},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(int(tt.n)), func(t *testing.T) {
			t.Parallel()

			if got := humanize.Comma(tt.n); got != tt.want {
				t.Errorf("Comma() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1_000, "1k"},
		{1_234, "1k"},
		{1_234_567, "1m"},
		// {1_234_567_890, "1g"}, // This test case is not supported
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.n), func(t *testing.T) {
			t.Parallel()

			if got := humanize.Unit(tt.n); got != tt.want {
				t.Errorf("Unit() = %v, want %v", got, tt.want)
			}
		})
	}
}
