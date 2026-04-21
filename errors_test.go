package dbx_test

import (
	"errors"
	"testing"
)

func TestStructuredErrors_Is(t *testing.T) {
	t.Run("PrimaryKeyUnmappedError", func(t *testing.T) {
		err := &PrimaryKeyUnmappedError{Column: "id"}
		if !errors.Is(err, ErrPrimaryKeyUnmapped) {
			t.Error("errors.Is(err, ErrPrimaryKeyUnmapped) should be true")
		}
	})

	t.Run("UnmappedColumnError", func(t *testing.T) {
		err := &UnmappedColumnError{Column: "missing_col"}
		if !errors.Is(err, ErrUnmappedColumn) {
			t.Error("errors.Is(err, ErrUnmappedColumn) should be true")
		}
	})
}

func TestStructuredErrors_As(t *testing.T) {
	assertStructuredErrorAs(t, "PrimaryKeyUnmappedError", &PrimaryKeyUnmappedError{Column: "role_id"}, func(target *PrimaryKeyUnmappedError) {
		if target.Column != "role_id" {
			t.Errorf("expected Column=%q, got %q", "role_id", target.Column)
		}
	})
	assertStructuredErrorAs(t, "UnmappedColumnError", &UnmappedColumnError{Column: "deleted_at"}, func(target *UnmappedColumnError) {
		if target.Column != "deleted_at" {
			t.Errorf("expected Column=%q, got %q", "deleted_at", target.Column)
		}
	})
}

func assertStructuredErrorAs[T error](t *testing.T, name string, err error, check func(T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		var target T
		if !errors.As(err, &target) {
			t.Fatal("errors.As should succeed")
		}
		check(target)
	})
}
