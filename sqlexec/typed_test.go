package sqlexec_test

import (
	"context"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
)

func TestSQLListTypedScansStructMapper(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1)`,
	)
	defer cleanup()

	type params struct {
		Status int64
	}

	statement := sqlstmt.NewTyped("user.find_active", func(value params) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{
			SQL:  `SELECT "id", "username" FROM "users" WHERE "status" = ?`,
			Args: collectionx.NewList[any](value.Status),
		}, nil
	})

	items, err := sqlexec.ListTyped[params, UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), statement, params{Status: 1}, MustStructMapper[UserSummary]())
	if err != nil {
		t.Fatalf("sqlexec.ListTyped returned error: %v", err)
	}
	first, _ := items.GetFirst()
	if items.Len() != 1 || first.Username != "alice" {
		t.Fatalf("unexpected typed items: %+v", items.Values())
	}
}
