package sqlexec_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
)

func TestSQLListUsesStatementCapacityHint(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	statement := sqlstmt.New("user.find_active", func(_ any) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{
			SQL:          `SELECT "id", "username" FROM "users" WHERE "status" = ?`,
			Args:         collectionx.NewList[any](int64(1)),
			CapacityHint: 2,
		}, nil
	})
	mapper := &capacityHintUserSummaryMapper{base: MustStructMapper[UserSummary]()}

	items, err := sqlexec.List[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), statement, nil, mapper)
	if err != nil {
		t.Fatalf("sqlexec.List returned error: %v", err)
	}
	if items.Len() != 2 {
		t.Fatalf("unexpected list size: %d", items.Len())
	}
	if mapper.capacityHint != 2 {
		t.Fatalf("sqlexec.List did not propagate capacity hint, got %d", mapper.capacityHint)
	}
	if mapper.scanRowsCalled {
		t.Fatalf("sqlexec.List used ScanRows instead of ScanRowsWithCapacity")
	}
}

func TestSQLGetUsesLimitScanner(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	statement := sqlstmt.New("user.find_active", func(_ any) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{
			SQL:  `SELECT "id", "username" FROM "users" WHERE "status" = ? ORDER BY "id"`,
			Args: collectionx.NewList[any](int64(1)),
		}, nil
	})
	mapper := &capacityHintUserSummaryMapper{base: MustStructMapper[UserSummary]()}

	_, err := sqlexec.Get[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), statement, nil, mapper)
	if !errors.Is(err, sqlexec.ErrTooManyRows) {
		t.Fatalf("expected sqlexec.ErrTooManyRows, got %v", err)
	}
	if mapper.limit != 2 {
		t.Fatalf("expected limit scanner to receive limit 2, got %d", mapper.limit)
	}
	if mapper.scanRowsCalled {
		t.Fatalf("sqlexec.Get used full ScanRows instead of ScanRowsLimit")
	}
}

type capacityHintUserSummaryMapper struct {
	base           StructMapper[UserSummary]
	capacityHint   int
	limit          int
	scanRowsCalled bool
}

func (m *capacityHintUserSummaryMapper) ScanRows(rows *sql.Rows) (*collectionx.List[UserSummary], error) {
	m.scanRowsCalled = true
	items, err := m.base.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	return items, nil
}

func (m *capacityHintUserSummaryMapper) ScanRowsWithCapacity(rows *sql.Rows, capacityHint int) (*collectionx.List[UserSummary], error) {
	m.capacityHint = capacityHint
	items, err := m.base.ScanRowsWithCapacity(rows, capacityHint)
	if err != nil {
		return nil, fmt.Errorf("scan rows with capacity: %w", err)
	}
	return items, nil
}

func (m *capacityHintUserSummaryMapper) ScanRowsLimit(ctx context.Context, rows *sql.Rows, limit int) (*collectionx.List[UserSummary], error) {
	m.limit = limit
	items, err := m.base.ScanRowsLimit(ctx, rows, limit)
	if err != nil {
		return nil, fmt.Errorf("scan rows limit: %w", err)
	}
	return items, nil
}
