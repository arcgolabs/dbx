package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/migrate"
	_ "modernc.org/sqlite"
)

func BenchmarkFileSourceList(b *testing.B) {
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_roles.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE roles (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_users.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"sql/U2__drop_users.sql":          &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
			"sql/R__refresh_materialized.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		Dir: "sql",
	}

	b.ReportAllocs()
	for range b.N {
		items, err := source.List()
		if err != nil {
			b.Fatalf("List returned error: %v", err)
		}
		if items.Len() != 4 {
			b.Fatalf("unexpected migration count: %d", items.Len())
		}
	}
}

func BenchmarkRunnerPendingSQL(b *testing.B) {
	ctx := context.Background()
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
			"sql/R__refresh_cache.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		Dir: "sql",
	}

	run := func(b *testing.B, db *sql.DB) {
		b.Helper()
		runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			items, err := runner.PendingSQL(ctx, source)
			if err != nil {
				b.Fatalf("PendingSQL returned error: %v", err)
			}
			if items.Len() != 2 {
				b.Fatalf("unexpected pending count: %d", items.Len())
			}
		}
	}

	b.Run("Memory", func(b *testing.B) { run(b, benchmarkOpenRunnerSQLiteDBMemory(b)) })
	b.Run("IO", func(b *testing.B) { run(b, benchmarkOpenRunnerSQLiteDB(b, "pending")) })
}

func BenchmarkRunnerApplied(b *testing.B) {
	ctx := context.Background()
	b.Run("Memory", func(b *testing.B) {
		benchmarkRunnerAppliedCase(ctx, b, benchmarkOpenRunnerSQLiteDBMemory(b))
	})
	b.Run("IO", func(b *testing.B) {
		benchmarkRunnerAppliedCase(ctx, b, benchmarkOpenRunnerSQLiteDB(b, "applied"))
	})
}

func benchmarkRunnerAppliedCase(ctx context.Context, b *testing.B, db *sql.DB) {
	b.Helper()
	runner := benchmarkPreparedRunner(ctx, b, db)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		items, err := runner.Applied(ctx)
		if err != nil {
			b.Fatalf("Applied returned error: %v", err)
		}
		if items.Len() != 2 {
			b.Fatalf("unexpected applied count: %d", items.Len())
		}
	}
}

func benchmarkPreparedRunner(ctx context.Context, b *testing.B, db *sql.DB) *migrate.Runner {
	b.Helper()
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})
	if _, err := runner.UpGo(ctx, benchmarkCreateSampleMigration()); err != nil {
		b.Fatalf("UpGo returned error: %v", err)
	}
	if _, err := runner.UpSQL(ctx, benchmarkSeedSampleSource()); err != nil {
		b.Fatalf("UpSQL returned error: %v", err)
	}
	return runner
}

func benchmarkCreateSampleMigration() migrate.GoMigration {
	return migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)
}

func benchmarkSeedSampleSource() migrate.FileSource {
	return migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V2__seed_sample.sql": &fstest.MapFile{Data: []byte("INSERT INTO sample (id) VALUES (1);\n")},
		},
		Dir: "sql",
	}
}

func benchmarkOpenRunnerSQLiteDB(b *testing.B, name string) *sql.DB {
	b.Helper()
	path := filepath.Join(b.TempDir(), name+".db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatalf("sql.Open returned error: %v", err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			b.Fatalf("close sqlite db: %v", closeErr)
		}
	})
	return db
}

func benchmarkOpenRunnerSQLiteDBMemory(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("sql.Open returned error: %v", err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			b.Fatalf("close sqlite db: %v", closeErr)
		}
	})
	return db
}
