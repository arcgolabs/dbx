package dbx_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

var errBenchmarkMissingResult = errors.New("benchmark result is missing")

func BenchmarkGetTyped(b *testing.B) {
	benchmarkTypedUserFetch(b, func(users UserSchema) func(context.Context, Session) error {
		query := SelectInto[UserSummary](users.ID, users.Username).
			From(users).
			Where(users.ID.Eq(1))
		return func(ctx context.Context, session Session) error {
			_, err := GetTyped[UserSummary](ctx, session, query)
			if err != nil {
				return fmt.Errorf("GetTyped returned error: %w", err)
			}
			return nil
		}
	})
}

func BenchmarkQueryOneTyped(b *testing.B) {
	benchmarkTypedUserFetch(b, func(users UserSchema) func(context.Context, Session) error {
		mapper := MustStructMapper[UserSummary]()
		query := SelectInto[UserSummary](users.ID, users.Username).
			From(users).
			Where(users.ID.Eq(1))
		return func(ctx context.Context, session Session) error {
			_, err := QueryOneTyped[UserSummary](ctx, session, query, mapper)
			if err != nil {
				return fmt.Errorf("QueryOneTyped returned error: %w", err)
			}
			return nil
		}
	})
}

func BenchmarkFindTyped(b *testing.B) {
	benchmarkTypedUserFetch(b, func(users UserSchema) func(context.Context, Session) error {
		query := SelectInto[UserSummary](users.ID, users.Username).
			From(users).
			Where(users.ID.Eq(1))
		return func(ctx context.Context, session Session) error {
			result, err := FindTyped[UserSummary](ctx, session, query)
			if err != nil {
				return fmt.Errorf("FindTyped returned error: %w", err)
			}
			if result.IsAbsent() {
				return errBenchmarkMissingResult
			}
			return nil
		}
	})
}

func BenchmarkQueryScalar(b *testing.B) {
	benchmarkTypedUserFetch(b, func(users UserSchema) func(context.Context, Session) error {
		query := SelectValue(users.ID).
			From(users).
			Where(users.ID.Eq(1))
		return func(ctx context.Context, session Session) error {
			_, err := QueryScalar[int64](ctx, session, query)
			if err != nil {
				return fmt.Errorf("QueryScalar returned error: %w", err)
			}
			return nil
		}
	})
}

func benchmarkTypedUserFetch(b *testing.B, prepare func(UserSchema) func(context.Context, Session) error) {
	b.Helper()
	dataSQL := []string{
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("id","username","email_address","status","role_id") VALUES (1,'alice','a@x.com',1,1)`,
	}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		users := MustSchema("users", UserSchema{})
		ctx := context.Background()
		runQuery := prepare(users)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := runQuery(ctx, core); err != nil {
				b.Fatal(err)
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
