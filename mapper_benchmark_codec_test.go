package dbx_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func BenchmarkQueryAllStructMapperJSONCodec(b *testing.B) {
	registerCSVCodecBenchmark()
	codecAccounts := MustSchema("codec_accounts", codecSchema{})
	mapper := MustStructMapper[codecRecord]()
	query := Select(AllColumns(codecAccounts).Values()...).From(codecAccounts)
	ddl := []string{mapperCodecExtraDDL, `INSERT INTO "codec_accounts" ("id","preferences","tags") VALUES (1,'{"theme":"dark","flags":["alpha","beta"]}','go,dbx,orm')`}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := QueryAll[codecRecord](context.Background(), core, query, mapper); err != nil {
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

func BenchmarkMapperInsertAssignmentsCodec(b *testing.B) {
	registerCSVCodecBenchmark()
	accounts := MustSchema("codec_accounts", codecSchema{})
	mapper := MustMapper[codecRecord](accounts)
	entity := &codecRecord{
		Preferences: codecPreferences{Theme: "dark", Flags: []string{"admin", "beta"}},
		Tags:        []string{"alpha", "beta"},
	}

	b.ReportAllocs()
	for range b.N {
		if _, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), accounts, entity); err != nil {
			b.Fatalf("InsertAssignments returned error: %v", err)
		}
	}
}

func registerCSVCodecBenchmark() {
	registerCSVCodecOnce.Do(func() {
		MustRegisterCodec(NewCodec[[]string](
			"csv",
			func(src any) ([]string, error) {
				switch value := src.(type) {
				case string:
					return splitCSV(value), nil
				case []byte:
					return splitCSV(string(value)), nil
				default:
					return nil, errors.New("dbx: csv codec only supports string or []byte")
				}
			},
			func(values []string) (any, error) {
				return strings.Join(values, ","), nil
			},
		))
	})
}
