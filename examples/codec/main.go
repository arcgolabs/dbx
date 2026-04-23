// Package main demonstrates dbx custom codec usage.
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
	codecx "github.com/arcgolabs/dbx/codec"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/schemamigrate"
)

type preferences struct {
	Theme string   `json:"theme"`
	Flags []string `json:"flags"`
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
		return nil, fmt.Errorf("invalid account status %q", s)
	}
}

func (s *accountStatus) UnmarshalText(text []byte) error {
	value := accountStatus(strings.ToLower(strings.TrimSpace(string(text))))
	switch value {
	case accountStatusActive, accountStatusBlocked:
		*s = value
		return nil
	default:
		return fmt.Errorf("invalid account status %q", value)
	}
}

type account struct {
	ID          int64         `dbx:"id"`
	Username    string        `dbx:"username"`
	Status      accountStatus `dbx:"status,codec=text"`
	CreatedAt   time.Time     `dbx:"created_at,codec=unix_milli_time"`
	Preferences preferences   `dbx:"preferences,codec=json"`
	Tags        []string      `dbx:"tags,codec=csv"`
}

type accountSchema struct {
	schemax.Schema[account]
	ID          columnx.Column[account, int64]         `dbx:"id,pk,auto"`
	Username    columnx.Column[account, string]        `dbx:"username,unique"`
	Status      columnx.Column[account, accountStatus] `dbx:"status,type=text"`
	CreatedAt   columnx.Column[account, time.Time]     `dbx:"created_at,type=integer"`
	Preferences columnx.Column[account, preferences]   `dbx:"preferences,type=text"`
	Tags        columnx.Column[account, []string]      `dbx:"tags,type=text"`
}

func main() {
	ctx := context.Background()
	logger := shared.NewLogger()
	core, closeDB, err := shared.OpenSQLite(
		"dbx-codec",
		dbx.WithLogger(logger),
		dbx.WithDebug(true),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeErr := closeDB(); closeErr != nil {
			panic(closeErr)
		}
	}()

	accounts := schemax.MustSchema("accounts", accountSchema{})
	_, err = schemamigrate.AutoMigrate(ctx, core, accounts)
	if err != nil {
		panic(err)
	}

	mapper := mapperx.MustMapperWithOptions[account](accounts, mapperx.WithMapperCodecs(newCSVCodec()))
	insertAccounts(ctx, core, accounts, mapper)

	items, err := queryAccounts(ctx, core, accounts, mapper)
	if err != nil {
		panic(err)
	}

	printAccounts(items)
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

func newCSVCodec() codecx.Codec {
	return codecx.New[[]string](
		"csv",
		func(src any) ([]string, error) {
			switch value := src.(type) {
			case string:
				return splitCSV(value), nil
			case []byte:
				return splitCSV(string(value)), nil
			default:
				return nil, fmt.Errorf("csv codec only supports string or []byte, got %T", src)
			}
		},
		func(values []string) (any, error) {
			return strings.Join(values, ","), nil
		},
	)
}

func insertAccounts(
	ctx context.Context,
	session dbx.Session,
	schema accountSchema,
	mapper mapperx.Mapper[account],
) {
	accountsToInsert := []*account{
		{
			Username:  "alice",
			Status:    accountStatusActive,
			CreatedAt: time.UnixMilli(1711111111222).UTC(),
			Preferences: preferences{
				Theme: "dark",
				Flags: []string{"beta", "admin"},
			},
			Tags: []string{"go", "dbx", "codec"},
		},
		{
			Username:  "bob",
			Status:    accountStatusBlocked,
			CreatedAt: time.UnixMilli(1712222222333).UTC(),
			Preferences: preferences{
				Theme: "light",
				Flags: []string{"reader"},
			},
			Tags: []string{"sqlite", "json"},
		},
	}

	for _, item := range accountsToInsert {
		assignments, err := mapper.InsertAssignments(session, schema, item)
		if err != nil {
			panic(err)
		}
		if _, err = dbx.Exec(ctx, session, querydsl.InsertInto(schema).Values(assignments.Values()...)); err != nil {
			panic(err)
		}
	}
}

func queryAccounts(
	ctx context.Context,
	session dbx.Session,
	schema accountSchema,
	mapper mapperx.Mapper[account],
) (collectionx.List[account], error) {
	return dbx.QueryAll[account](
		ctx,
		session,
		querydsl.Select(querydsl.AllColumns(schema).Values()...).From(schema).OrderBy(schema.ID.Asc()),
		mapper,
	)
}

func printAccounts(items collectionx.List[account]) {
	printLine("codec example:")
	items.Range(func(_ int, item account) bool {
		printFormat(
			"- id=%d username=%s status=%s created_at=%s theme=%s tags=%v\n",
			item.ID,
			item.Username,
			item.Status,
			item.CreatedAt.Format(time.RFC3339),
			item.Preferences.Theme,
			item.Tags,
		)
		return true
	})
}

func printLine(text string) {
	if _, err := fmt.Println(text); err != nil {
		panic(err)
	}
}

func printFormat(format string, args ...any) {
	if _, err := fmt.Printf(format, args...); err != nil {
		panic(err)
	}
}
