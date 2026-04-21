package dbx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	_ "modernc.org/sqlite"
)

func TestOpenValidation(t *testing.T) {
	t.Run("missing driver", func(t *testing.T) {
		_, err := dbx.Open(
			dbx.WithDSN("file:test?mode=memory"),
			dbx.WithDialect(sqlite.New()),
		)
		if !errors.Is(err, dbx.ErrMissingDriver) {
			t.Fatalf("expected ErrMissingDriver, got %v", err)
		}
	})
	t.Run("missing dsn", func(t *testing.T) {
		_, err := dbx.Open(
			dbx.WithDriver("sqlite"),
			dbx.WithDialect(sqlite.New()),
		)
		if !errors.Is(err, dbx.ErrMissingDSN) {
			t.Fatalf("expected ErrMissingDSN, got %v", err)
		}
	})
	t.Run("missing dialect", func(t *testing.T) {
		_, err := dbx.Open(
			dbx.WithDriver("sqlite"),
			dbx.WithDSN("file:test?mode=memory"),
		)
		if !errors.Is(err, dbx.ErrMissingDialect) {
			t.Fatalf("expected ErrMissingDialect, got %v", err)
		}
	})
}

func TestOpenSuccess(t *testing.T) {
	db, err := dbx.Open(
		dbx.WithDriver("sqlite"),
		dbx.WithDSN("file:TestOpenSuccess?mode=memory&cache=shared"),
		dbx.WithDialect(sqlite.New()),
		dbx.ApplyOptions(dbx.WithDebug(true)),
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("db.Close returned error: %v", closeErr)
		}
	}()

	if db.Dialect() == nil {
		t.Error("expected dialect")
	}
	if !db.Debug() {
		t.Error("expected debug true")
	}
	if db.SQLDB() == nil {
		t.Error("expected internal sql.DB")
	}

	// Sanity: can execute
	_, err = db.ExecContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}
}
