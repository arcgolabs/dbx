package sqlexec_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/sqlstmt"
)

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

type UserSummary struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

type DB = dbx.DB
type Cursor[E any] = dbx.Cursor[E]
type HookEvent = dbx.HookEvent
type HookFuncs = dbx.HookFuncs
type Operation = dbx.Operation
type Session = dbx.Session
type StructMapper[E any] = mapperx.StructMapper[E]

var MustNewWithOptions = dbx.MustNewWithOptions
var New = dbx.New
var OperationQuery = dbx.OperationQuery
var WithHooks = dbx.WithHooks

func MustStructMapper[E any]() StructMapper[E] {
	return mapperx.MustStructMapper[E]()
}

func SQLCursor[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper mapperx.RowsScanner[E]) (Cursor[E], error) {
	cursor, err := dbx.SQLCursor[E](ctx, session, statement, params, mapper)
	if err != nil {
		return nil, fmt.Errorf("sql cursor: %w", err)
	}
	return cursor, nil
}

func SQLEach[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper mapperx.RowsScanner[E]) func(func(E, error) bool) {
	return dbx.SQLEach[E](ctx, session, statement, params, mapper)
}

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
	for _, statement := range ddl {
		if statement == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			closeTestSQLite(tb, db)
			tb.Fatalf("exec ddl %q: %v", statement, err)
		}
	}
	return db, func() { closeTestSQLite(tb, db) }
}

func OpenTestSQLiteWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return OpenTestSQLite(tb, ddl...)
}

func OpenBenchmarkSQLiteMemoryWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return openBenchmarkSQLite(tb, ":memory:", ddl...)
}

func OpenBenchmarkSQLiteWithSchema(tb testing.TB, dataSQL ...string) (*sql.DB, func()) {
	tb.Helper()
	ddl := make([]string, 0, 1+len(dataSQL))
	ddl = append(ddl, testSchemaDDL)
	ddl = append(ddl, dataSQL...)
	return openBenchmarkSQLite(tb, filepath.Join(tb.TempDir(), "bench.db"), ddl...)
}

func openBenchmarkSQLite(tb testing.TB, dsn string, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		closeTestSQLite(tb, db)
		tb.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	for _, statement := range ddl {
		if statement == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			closeTestSQLite(tb, db)
			tb.Fatalf("exec ddl %q: %v", statement, err)
		}
	}
	return db, func() { closeTestSQLite(tb, db) }
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

func closeCursorOrFatal[E any](t *testing.T, cursor Cursor[E]) {
	t.Helper()
	if err := cursor.Close(); err != nil {
		t.Fatalf("cursor.Close returned error: %v", err)
	}
}

func collectUserSummaryCursor(t *testing.T, cursor Cursor[UserSummary]) []UserSummary {
	t.Helper()
	var items []UserSummary
	for cursor.Next() {
		item, err := cursor.Get()
		if err != nil {
			t.Fatalf("cursor.Get returned error: %v", err)
		}
		items = append(items, item)
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor.Err returned error: %v", err)
	}
	return items
}

func assertUserSummaryRows(t *testing.T, items []UserSummary) {
	t.Helper()
	if len(items) != 2 || items[0].Username != "alice" || items[1].ID != 2 {
		t.Fatalf("unexpected items: %+v", items)
	}
}

type testSQLiteDialect struct{}

func (testSQLiteDialect) Name() string         { return "sqlite" }
func (testSQLiteDialect) BindVar(_ int) string { return "?" }
func (testSQLiteDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (testSQLiteDialect) RenderLimitOffset(limit, offset *int) (string, error) {
	if limit == nil && offset == nil {
		return "", nil
	}
	if limit != nil && offset != nil {
		return fmt.Sprintf("LIMIT %d OFFSET %d", *limit, *offset), nil
	}
	if limit != nil {
		return fmt.Sprintf("LIMIT %d", *limit), nil
	}
	return fmt.Sprintf("LIMIT -1 OFFSET %d", *offset), nil
}

var _ dialect.Dialect = testSQLiteDialect{}
