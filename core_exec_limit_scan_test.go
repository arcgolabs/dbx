package dbx_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	mapperx "github.com/arcgolabs/dbx/mapper"
)

func TestQueryCursorUsesCursorScanner(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	mapper := &limitTrackingUserSummaryMapper{base: MustStructMapper[UserSummary]()}
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc())

	cursor, err := QueryCursor[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query, mapper)
	if err != nil {
		t.Fatalf("QueryCursor returned error: %v", err)
	}
	defer closeCursorOrFatal(t, cursor)
	assertUserSummaryRows(t, collectUserSummaryCursor(t, cursor))

	if !mapper.scanCursorCalled {
		t.Fatalf("QueryCursor did not use ScanCursor")
	}
	if mapper.scanRowsCalled {
		t.Fatalf("QueryCursor used full ScanRows instead of ScanCursor")
	}
}

func TestQueryOneTypedUsesLimitScanner(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	mapper := &limitTrackingUserSummaryMapper{base: MustStructMapper[UserSummary]()}
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc())

	_, err := QueryOneTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query, mapper)
	if !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("expected ErrTooManyRows, got %v", err)
	}
	if mapper.limit != 2 {
		t.Fatalf("expected limit scanner to receive limit 2, got %d", mapper.limit)
	}
	if mapper.scanRowsCalled {
		t.Fatalf("QueryOneTyped used full ScanRows instead of ScanRowsLimit")
	}
}

type limitTrackingUserSummaryMapper struct {
	base             StructMapper[UserSummary]
	limit            int
	scanCursorCalled bool
	scanRowsCalled   bool
}

func (m *limitTrackingUserSummaryMapper) ScanRows(rows *sql.Rows) (*collectionx.List[UserSummary], error) {
	m.scanRowsCalled = true
	items, err := m.base.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	return items, nil
}

func (m *limitTrackingUserSummaryMapper) ScanCursor(ctx context.Context, rows *sql.Rows) (mapperx.Cursor[UserSummary], error) {
	m.scanCursorCalled = true
	cursor, err := m.base.ScanCursor(ctx, rows)
	if err != nil {
		return nil, fmt.Errorf("scan cursor: %w", err)
	}
	return cursor, nil
}

func (m *limitTrackingUserSummaryMapper) ScanRowsLimit(ctx context.Context, rows *sql.Rows, limit int) (*collectionx.List[UserSummary], error) {
	m.limit = limit
	items, err := m.base.ScanRowsLimit(ctx, rows, limit)
	if err != nil {
		return nil, fmt.Errorf("scan rows limit: %w", err)
	}
	return items, nil
}
