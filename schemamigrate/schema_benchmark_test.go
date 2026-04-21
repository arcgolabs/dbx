package schemamigrate_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func BenchmarkCompileAtlasSchema(b *testing.B) {
	roles := MustSchema("roles", advancedRoleSchema{})
	users := MustSchema("users", advancedUserSchema{})
	schemas := []SchemaResource{roles, users}

	b.ReportAllocs()
	for range b.N {
		if compiled := CompileAtlasSchemaForTest("sqlite", schemas...); compiled == nil {
			b.Fatal("CompileAtlasSchemaForTest returned nil")
		}
	}
}

func BenchmarkPlanSchemaChangesSQLiteAtlasEmpty(b *testing.B) {
	ctx := context.Background()
	roles := MustSchema("roles", RoleSchema{})
	users := MustSchema("users", UserSchema{})

	run := func(b *testing.B, db *sql.DB) {
		b.Helper()
		core := New(db, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanSchemaChanges(ctx, core, roles, users); err != nil {
				b.Fatalf("PlanSchemaChanges returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDBMemory(b))
	})
	b.Run("IO", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDB(b, "plan-empty"))
	})
}

func BenchmarkValidateSchemasSQLiteAtlasMatched(b *testing.B) {
	ctx := context.Background()
	roles := MustSchema("roles", RoleSchema{})
	users := MustSchema("users", UserSchema{})

	run := func(b *testing.B, db *sql.DB) {
		b.Helper()
		core := New(db, testSQLiteDialect{})
		if _, err := AutoMigrate(ctx, core, roles, users); err != nil {
			b.Fatalf("AutoMigrate returned error: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ValidateSchemas(ctx, core, roles, users); err != nil {
				b.Fatalf("ValidateSchemas returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDBMemory(b))
	})
	b.Run("IO", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDB(b, "validate-matched"))
	})
}

func BenchmarkMigrationPlanSQLPreview(b *testing.B) {
	ctx := context.Background()
	roles := MustSchema("roles", RoleSchema{})
	users := MustSchema("users", UserSchema{})

	run := func(b *testing.B, db *sql.DB) {
		b.Helper()
		core := New(db, testSQLiteDialect{})
		plan, err := PlanSchemaChanges(ctx, core, roles, users)
		if err != nil {
			b.Fatalf("PlanSchemaChanges returned error: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			preview := plan.SQLPreview()
			first, ok := preview.Get(0)
			if !ok || !strings.Contains(strings.ToLower(first), "create table") {
				b.Fatalf("unexpected preview: %+v", preview)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDBMemory(b))
	})
	b.Run("IO", func(b *testing.B) {
		run(b, benchmarkOpenSQLiteDB(b, "preview"))
	})
}

func benchmarkOpenSQLiteDB(b *testing.B, name string) *sql.DB {
	b.Helper()
	path := filepath.Join(b.TempDir(), name+".db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatalf("sql.Open returned error: %v", err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			b.Fatalf("db.Close returned error: %v", closeErr)
		}
	})
	return db
}

func benchmarkOpenSQLiteDBMemory(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("sql.Open returned error: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			b.Fatalf("db.Close returned error after PRAGMA failure: %v", closeErr)
		}
		b.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			b.Fatalf("db.Close returned error: %v", closeErr)
		}
	})
	return db
}
