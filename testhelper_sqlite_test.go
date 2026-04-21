package dbx_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// Standard DDL for users/roles schema used by most tests.
const testSchemaDDL = `
CREATE TABLE IF NOT EXISTS "roles" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS "users" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"username" TEXT NOT NULL,
	"email_address" TEXT NOT NULL,
	"status" INTEGER NOT NULL DEFAULT 1,
	"role_id" INTEGER NOT NULL REFERENCES "roles"("id") ON DELETE CASCADE
);
`

// OpenTestSQLite opens an in-memory SQLite DB, runs ddl statements, and returns the DB plus cleanup.
// Call cleanup() when done.
func OpenTestSQLite(tb testing.TB, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		closeTestSQLite(tb, db)
		tb.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	for _, s := range ddl {
		if s == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			closeTestSQLite(tb, db)
			tb.Fatalf("exec ddl %q: %v", s, err)
		}
	}
	return db, func() { closeTestSQLite(tb, db) }
}

// OpenTestSQLiteWithSchema opens in-memory SQLite with the standard users/roles schema and optional data SQL.
func OpenTestSQLiteWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return OpenTestSQLite(tb, ddl...)
}

// OpenBenchmarkSQLiteMemory opens an in-memory SQLite DB for benchmarks (no disk I/O).
// Use with b.Run("Memory", ...) to compare against OpenBenchmarkSQLite (disk I/O).
func OpenBenchmarkSQLiteMemory(tb testing.TB, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		closeTestSQLite(tb, db)
		tb.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	for _, s := range ddl {
		if s == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			closeTestSQLite(tb, db)
			tb.Fatalf("exec ddl %q: %v", s, err)
		}
	}
	return db, func() { closeTestSQLite(tb, db) }
}

// OpenBenchmarkSQLiteMemoryWithSchema opens in-memory SQLite with standard users/roles schema for benchmarks.
func OpenBenchmarkSQLiteMemoryWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return OpenBenchmarkSQLiteMemory(tb, ddl...)
}

// OpenBenchmarkSQLite opens a SQLite DB file in a temp directory (real disk I/O).
// Use for benchmarks to simulate production; use OpenBenchmarkSQLiteMemory for in-memory comparison.
func OpenBenchmarkSQLite(tb testing.TB, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "bench.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		closeTestSQLite(tb, db)
		tb.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	for _, s := range ddl {
		if s == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			closeTestSQLite(tb, db)
			tb.Fatalf("exec ddl %q: %v", s, err)
		}
	}
	return db, func() { closeTestSQLite(tb, db) }
}

// OpenBenchmarkSQLiteWithSchema opens SQLite in temp dir with standard users/roles schema and optional data.
func OpenBenchmarkSQLiteWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return OpenBenchmarkSQLite(tb, ddl...)
}

func closeTestSQLite(tb testing.TB, db *sql.DB) {
	tb.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		tb.Fatalf("db.Close: %v", err)
	}
}
