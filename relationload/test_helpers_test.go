package relationload_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arcgolabs/collectionx"
	_ "modernc.org/sqlite"

	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect"
	mapperx "github.com/arcgolabs/dbx/mapper"
	relationx "github.com/arcgolabs/dbx/relation"
	relationload "github.com/arcgolabs/dbx/relationload"
	schemax "github.com/arcgolabs/dbx/schema"
)

type Schema[E any] = schemax.Schema[E]
type Column[E any, T any] = columnx.Column[E, T]
type Mapper[E any] = mapperx.Mapper[E]
type SchemaSource[E any] = schemax.SchemaSource[E]
type Session = dbx.Session

var ErrRelationCardinality = dbx.ErrRelationCardinality
var New = dbx.New

func LoadBelongsTo[S any, T any](ctx context.Context, session Session, sources collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.BelongsTo[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.SingleRelationAssigner[S, T]) error {
	if err := relationload.LoadBelongsTo(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load belongs-to relation: %w", err)
	}
	return nil
}

func LoadHasMany[S any, T any](ctx context.Context, session Session, sources collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.HasMany[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.MultiRelationAssigner[S, T]) error {
	if err := relationload.LoadHasMany(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load has-many relation: %w", err)
	}
	return nil
}

func LoadHasOne[S any, T any](ctx context.Context, session Session, sources collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.HasOne[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.SingleRelationAssigner[S, T]) error {
	if err := relationload.LoadHasOne(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load has-one relation: %w", err)
	}
	return nil
}

func LoadManyToMany[S any, T any](ctx context.Context, session Session, sources collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.ManyToMany[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.MultiRelationAssigner[S, T]) error {
	if err := relationload.LoadManyToMany(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load many-to-many relation: %w", err)
	}
	return nil
}

func MustMapper[E any](schema schemax.Resource) Mapper[E] {
	return mapperx.MustMapper[E](schema)
}

func MustSchema[S any](name string, schema S) S {
	return schemax.MustSchema(name, schema)
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

func OpenBenchmarkSQLiteMemory(tb testing.TB, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
	return openBenchmarkSQLite(tb, ":memory:", ddl...)
}

func OpenBenchmarkSQLite(tb testing.TB, ddl ...string) (*sql.DB, func()) {
	tb.Helper()
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
