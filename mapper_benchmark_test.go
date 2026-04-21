package dbx_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"testing"
)

func BenchmarkNewStructMapperCached(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := NewStructMapper[auditedUser](); err != nil {
			b.Fatalf("NewStructMapper returned error: %v", err)
		}
	}
}

func BenchmarkStructMapperScanPlanCached(b *testing.B) {
	mapper := MustStructMapper[accountRecord]()
	columns := []string{"id", "nickname", "bio", "label"}

	b.ReportAllocs()
	for range b.N {
		if err := StructMapperScanPlanForTest(mapper, columns); err != nil {
			b.Fatalf("scanPlan returned error: %v", err)
		}
	}
}

func BenchmarkStructMapperScanPlanAliasFallback(b *testing.B) {
	mapper := MustStructMapper[auditedUser]()
	columns := []string{`"users"."id"`, `"CREATED_BY"`, `"UPDATED_BY"`}

	b.ReportAllocs()
	for range b.N {
		if err := StructMapperScanPlanForTest(mapper, columns); err != nil {
			b.Fatalf("scanPlan returned error: %v", err)
		}
	}
}

func BenchmarkMapperInsertAssignments(b *testing.B) {
	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustMapper[accountRecord](accounts)
	entity := &accountRecord{
		Label: "ADMIN",
	}

	b.ReportAllocs()
	for range b.N {
		if _, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), accounts, entity); err != nil {
			b.Fatalf("InsertAssignments returned error: %v", err)
		}
	}
}

type benchmarkDBAutoIDRecord struct {
	ID    int64  `dbx:"id"`
	Label string `dbx:"label"`
}

type benchmarkSnowflakeIDRecord struct {
	ID    int64  `dbx:"id"`
	Label string `dbx:"label"`
}

type benchmarkUUIDIDRecord struct {
	ID    string `dbx:"id"`
	Label string `dbx:"label"`
}

type benchmarkDBAutoIDSchema struct {
	Schema[benchmarkDBAutoIDRecord]
	ID    Column[benchmarkDBAutoIDRecord, int64]  `dbx:"id,pk"`
	Label Column[benchmarkDBAutoIDRecord, string] `dbx:"label"`
}

type benchmarkSnowflakeIDSchema struct {
	Schema[benchmarkSnowflakeIDRecord]
	ID    IDColumn[benchmarkSnowflakeIDRecord, int64, IDSnowflake] `dbx:"id,pk"`
	Label Column[benchmarkSnowflakeIDRecord, string]               `dbx:"label"`
}

type benchmarkUUIDIDSchema struct {
	Schema[benchmarkUUIDIDRecord]
	ID    IDColumn[benchmarkUUIDIDRecord, string, IDUUIDv7] `dbx:"id,pk"`
	Label Column[benchmarkUUIDIDRecord, string]             `dbx:"label"`
}

func BenchmarkMapperInsertAssignmentsIDStrategy(b *testing.B) {
	dbAutoSchema := MustSchema("benchmark_db_auto_records", benchmarkDBAutoIDSchema{})
	dbAutoMapper := MustMapper[benchmarkDBAutoIDRecord](dbAutoSchema)
	dbAutoEntity := &benchmarkDBAutoIDRecord{Label: "admin"}

	snowflakeSchema := MustSchema("benchmark_snowflake_records", benchmarkSnowflakeIDSchema{})
	snowflakeMapper := MustMapper[benchmarkSnowflakeIDRecord](snowflakeSchema)
	snowflakeEntity := &benchmarkSnowflakeIDRecord{Label: "admin"}

	uuidSchema := MustSchema("benchmark_uuid_records", benchmarkUUIDIDSchema{})
	uuidMapper := MustMapper[benchmarkUUIDIDRecord](uuidSchema)
	uuidEntity := &benchmarkUUIDIDRecord{Label: "admin"}

	runInsertAssignmentBenchmark(b, "DBAuto", func() { dbAutoEntity.ID = 0 }, func() error {
		_, err := dbAutoMapper.InsertAssignments(New(nil, testSQLiteDialect{}), dbAutoSchema, dbAutoEntity)
		if err != nil {
			return fmt.Errorf("db auto insert assignments: %w", err)
		}
		return nil
	})
	runInsertAssignmentBenchmark(b, "Snowflake", func() { snowflakeEntity.ID = 0 }, func() error {
		_, err := snowflakeMapper.InsertAssignments(New(nil, testSQLiteDialect{}), snowflakeSchema, snowflakeEntity)
		if err != nil {
			return fmt.Errorf("snowflake insert assignments: %w", err)
		}
		return nil
	})
	runInsertAssignmentBenchmark(b, "UUIDv7", func() { uuidEntity.ID = "" }, func() error {
		_, err := uuidMapper.InsertAssignments(New(nil, testSQLiteDialect{}), uuidSchema, uuidEntity)
		if err != nil {
			return fmt.Errorf("uuidv7 insert assignments: %w", err)
		}
		return nil
	})
}

func BenchmarkQueryAllStructMapper(b *testing.B) {
	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustStructMapper[accountRecord]()
	query := Select(AllColumns(accounts).Values()...).From(accounts)
	ddl := []string{mapperScanAccountsDDL, `INSERT INTO "accounts" ("id","nickname","bio","label") VALUES (1,'ally','hello','admin'),(2,NULL,NULL,'reader')`}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := QueryAll[accountRecord](context.Background(), core, query, mapper); err != nil {
				b.Fatalf("QueryAll returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}

func BenchmarkQueryAllStructMapperWithLimit(b *testing.B) {
	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustStructMapper[accountRecord]()
	query := Select(AllColumns(accounts).Values()...).From(accounts).Limit(20)
	ddl := []string{mapperScanAccountsDDL, `INSERT INTO "accounts" ("id","nickname","bio","label") VALUES (1,'ally','hello','admin'),(2,NULL,NULL,'reader')`}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := QueryAll[accountRecord](context.Background(), core, query, mapper); err != nil {
				b.Fatalf("QueryAll returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}

func BenchmarkQueryCursorStructMapper(b *testing.B) {
	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustStructMapper[accountRecord]()
	query := Select(AllColumns(accounts).Values()...).From(accounts)
	ddl := []string{mapperScanAccountsDDL, `INSERT INTO "accounts" ("id","nickname","bio","label") VALUES (1,'ally','hello','admin'),(2,NULL,NULL,'reader')`}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkQueryCursorOnce(b, core, query, mapper)
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}

func runInsertAssignmentBenchmark(b *testing.B, name string, reset func(), run func() error) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reset()
			if err := run(); err != nil {
				b.Fatalf("InsertAssignments returned error: %v", err)
			}
		}
	})
}

func benchmarkQueryCursorOnce(b *testing.B, core *DB, query *querydsl.SelectQuery, mapper StructMapper[accountRecord]) {
	b.Helper()
	cursor, err := QueryCursor[accountRecord](context.Background(), core, query, mapper)
	if err != nil {
		b.Fatalf("QueryCursor returned error: %v", err)
	}
	closeCursor := func() {
		if closeErr := cursor.Close(); closeErr != nil {
			b.Fatalf("cursor.Close returned error: %v", closeErr)
		}
	}
	for cursor.Next() {
		if _, err := cursor.Get(); err != nil {
			closeCursor()
			b.Fatalf("cursor.Get returned error: %v", err)
		}
	}
	if err := cursor.Err(); err != nil {
		closeCursor()
		b.Fatalf("cursor.Err returned error: %v", err)
	}
	closeCursor()
}

func BenchmarkSQLScalar(b *testing.B) {
	statement := sqlstmt.New("user.count", func(_ any) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{SQL: `SELECT count(*) FROM "users"`}, nil
	})
	dataSQL := []string{
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('a','a@x.com',1,1),('b','b@x.com',1,1)`,
	}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		db := New(sqlDB, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := sqlexec.Scalar[int64](context.Background(), db, statement, nil); err != nil {
				b.Fatalf("sqlexec.Scalar returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemoryWithSchema(b, dataSQL...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteWithSchema(b, dataSQL...)
		defer cleanup()
		run(b, sqlDB)
	})
}
