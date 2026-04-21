package sqlexec_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"testing"

	"github.com/DaiYuANg/arcgo/collectionx"
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

type capacityHintUserSummaryMapper struct {
	base           StructMapper[UserSummary]
	capacityHint   int
	scanRowsCalled bool
}

func (m *capacityHintUserSummaryMapper) ScanRows(rows *sql.Rows) (collectionx.List[UserSummary], error) {
	m.scanRowsCalled = true
	items, err := m.base.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	return items, nil
}

func (m *capacityHintUserSummaryMapper) ScanRowsWithCapacity(rows *sql.Rows, capacityHint int) (collectionx.List[UserSummary], error) {
	m.capacityHint = capacityHint
	items, err := m.base.ScanRowsWithCapacity(rows, capacityHint)
	if err != nil {
		return nil, fmt.Errorf("scan rows with capacity: %w", err)
	}
	return items, nil
}
