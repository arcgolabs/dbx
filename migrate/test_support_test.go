// Package migrate_test exercises the public migrate package API.
package migrate_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type testDialect struct{}

func (testDialect) Name() string                   { return "sqlite" }
func (testDialect) BindVar(_ int) string           { return "?" }
func (testDialect) QuoteIdent(ident string) string { return `"` + ident + `"` }
func (testDialect) RenderLimitOffset(_, _ *int) (string, error) {
	return "", nil
}

func openSQLiteRunnerDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()

	db, err := sql.Open("sqlite", path)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			tb.Errorf("close sqlite db: %v", closeErr)
		}
	})
	return db
}

func sqliteTableExists(ctx context.Context, tb testing.TB, db *sql.DB, name string) bool {
	tb.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, name).Scan(&exists)
	require.NoError(tb, err)
	return exists
}
