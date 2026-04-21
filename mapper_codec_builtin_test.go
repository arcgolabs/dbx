package dbx_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type timeCodecRecord struct {
	ID        int64     `dbx:"id"`
	CreatedAt time.Time `dbx:"created_at,codec=unix_milli_time"`
}

type timeCodecSchema struct {
	Schema[timeCodecRecord]
	ID        Column[timeCodecRecord, int64]     `dbx:"id,pk,auto"`
	CreatedAt Column[timeCodecRecord, time.Time] `dbx:"created_at"`
}

type accountStatus string

const (
	accountStatusActive  accountStatus = "active"
	accountStatusBlocked accountStatus = "blocked"
)

func (s accountStatus) MarshalText() ([]byte, error) {
	switch s {
	case accountStatusActive, accountStatusBlocked:
		return []byte(s), nil
	default:
		return nil, errors.New("dbx: invalid account status")
	}
}

func (s *accountStatus) UnmarshalText(text []byte) error {
	value := accountStatus(strings.ToLower(strings.TrimSpace(string(text))))
	switch value {
	case accountStatusActive, accountStatusBlocked:
		*s = value
		return nil
	default:
		return errors.New("dbx: invalid account status")
	}
}

type decimalAmount struct {
	text string
}

func (a decimalAmount) MarshalText() ([]byte, error) {
	if strings.TrimSpace(a.text) == "" {
		return nil, errors.New("dbx: empty decimal amount")
	}
	return []byte(a.text), nil
}

func (a *decimalAmount) UnmarshalText(text []byte) error {
	trimmed := strings.TrimSpace(string(text))
	if trimmed == "" {
		return errors.New("dbx: empty decimal amount")
	}
	a.text = trimmed
	return nil
}

func (a decimalAmount) String() string {
	return a.text
}

type textCodecRecord struct {
	ID      int64         `dbx:"id"`
	Status  accountStatus `dbx:"status,codec=text"`
	Balance decimalAmount `dbx:"balance,codec=text"`
}

type textCodecSchema struct {
	Schema[textCodecRecord]
	ID      Column[textCodecRecord, int64]         `dbx:"id,pk,auto"`
	Status  Column[textCodecRecord, accountStatus] `dbx:"status,type=text"`
	Balance Column[textCodecRecord, decimalAmount] `dbx:"balance,type=text"`
}

func TestBuiltInUnixMilliTimeCodecScanAndEncode(t *testing.T) {
	createdAt := time.UnixMilli(1711111111222).UTC()

	sqlDB, cleanup := OpenTestSQLite(t, mapperCodecExtraDDL,
		fmt.Sprintf(`INSERT INTO "time_codec_records" ("id","created_at") VALUES (1,%d)`, createdAt.UnixMilli()),
	)
	defer cleanup()

	schema := MustSchema("time_codec_records", timeCodecSchema{})
	mapper := MustMapper[timeCodecRecord](schema)

	items, err := QueryAll[timeCodecRecord](
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		Select(AllColumns(schema).Values()...).From(schema),
		mapper,
	)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	item, _ := items.GetFirst()
	if items.Len() != 1 || !item.CreatedAt.Equal(createdAt) {
		t.Fatalf("unexpected time codec items: %+v", items.Values())
	}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), schema, &item)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	rec := &hookRecorder{}
	if _, err := Exec(context.Background(), MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after})), InsertInto(schema).Values(assignments.Values()...)); err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected exec count: %d", rec.execCount)
	}
}

func TestBuiltInTextCodecScanAndEncode(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, mapperCodecExtraDDL,
		`INSERT INTO "text_codec_records" ("id","status","balance") VALUES (1,'active','123.45')`,
	)
	defer cleanup()

	schema := MustSchema("text_codec_records", textCodecSchema{})
	mapper := MustMapper[textCodecRecord](schema)

	items, err := QueryAll[textCodecRecord](
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		Select(AllColumns(schema).Values()...).From(schema),
		mapper,
	)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	if items.Len() != 1 {
		t.Fatalf("unexpected item count: %d", items.Len())
	}
	item, _ := items.GetFirst()
	if item.Status != accountStatusActive {
		t.Fatalf("unexpected status: %q", item.Status)
	}
	if item.Balance.String() != "123.45" {
		t.Fatalf("unexpected balance: %s", item.Balance.String())
	}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), schema, &item)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	rec := &hookRecorder{}
	if _, err := Exec(context.Background(), MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after})), InsertInto(schema).Values(assignments.Values()...)); err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected exec count: %d", rec.execCount)
	}
}
