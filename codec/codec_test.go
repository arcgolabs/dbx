package codec_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/arcgolabs/dbx/codec"
)

func TestUnknownError(t *testing.T) {
	err := &codec.UnknownError{Name: "csv"}
	if !errors.Is(err, codec.ErrUnknown) {
		t.Fatal("errors.Is(err, ErrUnknown) should be true")
	}

	wrapped := fmt.Errorf("mapper init: %w", err)
	var target *codec.UnknownError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should succeed on wrapped error")
	}
	if target.Name != "csv" {
		t.Fatalf("expected Name=%q, got %q", "csv", target.Name)
	}
}
