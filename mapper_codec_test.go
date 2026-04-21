package dbx_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type codecPreferences struct {
	Theme string   `json:"theme"`
	Flags []string `json:"flags"`
}

type codecRecord struct {
	ID          int64            `dbx:"id"`
	Preferences codecPreferences `dbx:"preferences,codec=json"`
	Tags        []string         `dbx:"tags,codec=csv"`
}

type codecSchema struct {
	Schema[codecRecord]
	ID          Column[codecRecord, int64]            `dbx:"id,pk,auto"`
	Preferences Column[codecRecord, codecPreferences] `dbx:"preferences"`
	Tags        Column[codecRecord, []string]         `dbx:"tags"`
}

type scopedCodecRecord struct {
	ID   int64    `dbx:"id"`
	Tags []string `dbx:"tags,codec=scoped_csv"`
}

type scopedCodecSchema struct {
	Schema[scopedCodecRecord]
	ID   Column[scopedCodecRecord, int64]    `dbx:"id,pk,auto"`
	Tags Column[scopedCodecRecord, []string] `dbx:"tags"`
}

var registerCSVCodecOnce sync.Once

const mapperCodecExtraDDL = `
CREATE TABLE IF NOT EXISTS "codec_accounts" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"preferences" TEXT NOT NULL,
	"tags" TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS "scoped_codec_records" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"tags" TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS "time_codec_records" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"created_at" INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS "text_codec_records" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"status" TEXT NOT NULL,
	"balance" TEXT NOT NULL
);
`

func registerCSVCodec(t *testing.T) {
	t.Helper()
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

func TestStructMapperScansCodecFields(t *testing.T) {
	registerCSVCodec(t)

	sqlDB, cleanup := OpenTestSQLite(t, mapperCodecExtraDDL,
		`INSERT INTO "codec_accounts" ("id","preferences","tags") VALUES (1,'{"theme":"dark","flags":["alpha","beta"]}','go,dbx,orm')`,
	)
	defer cleanup()

	accounts := MustSchema("codec_accounts", codecSchema{})
	items, err := QueryAll[codecRecord](
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		Select(AllColumns(accounts).Values()...).From(accounts),
		MustStructMapper[codecRecord](),
	)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	if items.Len() != 1 {
		t.Fatalf("unexpected item count: %d", items.Len())
	}
	item, _ := items.GetFirst()
	if item.Preferences.Theme != "dark" {
		t.Fatalf("unexpected preferences: %+v", item.Preferences)
	}
	if len(item.Tags) != 3 || item.Tags[1] != "dbx" {
		t.Fatalf("unexpected tags: %+v", item.Tags)
	}
}

func TestMapperAssignmentsUseCodecEncoding(t *testing.T) {
	registerCSVCodec(t)

	sqlDB, cleanup := OpenTestSQLite(t, mapperCodecExtraDDL)
	defer cleanup()

	accounts := MustSchema("codec_accounts", codecSchema{})
	mapper := MustMapper[codecRecord](accounts)
	entity := &codecRecord{
		Preferences: codecPreferences{
			Theme: "dark",
			Flags: []string{"admin", "beta"},
		},
		Tags: []string{"alpha", "beta"},
	}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), accounts, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if assignments.Len() != 2 {
		t.Fatalf("unexpected assignment count: %d", assignments.Len())
	}

	rec := &hookRecorder{}
	if _, err := Exec(context.Background(), MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after})), InsertInto(accounts).Values(assignments.Values()...)); err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected exec count: %d", rec.execCount)
	}
}

func TestNewStructMapperReturnsErrorForUnknownCodec(t *testing.T) {
	type invalidCodecRecord struct {
		ID   int64  `dbx:"id"`
		Data string `dbx:"data,codec=missing"`
	}

	_, err := NewStructMapper[invalidCodecRecord]()
	if !errors.Is(err, ErrUnknownCodec) {
		t.Fatalf("expected ErrUnknownCodec, got: %v", err)
	}
}

func TestStructMapperWithOptionsUsesScopedCodecRegistry(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, mapperCodecExtraDDL,
		`INSERT INTO "scoped_codec_records" ("id","tags") VALUES (1,'one,two')`,
	)
	defer cleanup()

	schema := MustSchema("scoped_codec_records", scopedCodecSchema{})
	scopedCSV := NewCodec[[]string](
		"scoped_csv",
		func(src any) ([]string, error) {
			switch value := src.(type) {
			case string:
				return splitCSV(value), nil
			case []byte:
				return splitCSV(string(value)), nil
			default:
				return nil, errors.New("dbx: scoped csv codec only supports string or []byte")
			}
		},
		func(values []string) (any, error) {
			return strings.Join(values, ","), nil
		},
	)

	mapper, err := NewStructMapperWithOptions[scopedCodecRecord](WithMapperCodecs(scopedCSV))
	if err != nil {
		t.Fatalf("NewStructMapperWithOptions returned error: %v", err)
	}

	items, err := QueryAll[scopedCodecRecord](
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		Select(AllColumns(schema).Values()...).From(schema),
		mapper,
	)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	item, _ := items.GetFirst()
	if items.Len() != 1 || len(item.Tags) != 2 || item.Tags[1] != "two" {
		t.Fatalf("unexpected scoped codec items: %+v", items.Values())
	}

	if _, err := NewStructMapper[scopedCodecRecord](); !errors.Is(err, ErrUnknownCodec) {
		t.Fatalf("expected default mapper to reject scoped codec tag, got: %v", err)
	}
}

func splitCSV(input string) []string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}
