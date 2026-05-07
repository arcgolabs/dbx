package dbx_test

import (
	"context"
	"testing"
)

func TestQueryAllTypedScansTypedSelect(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	mapper := MustMapper[UserSummary](users)
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc())

	items, err := QueryAllTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query, mapper)
	if err != nil {
		t.Fatalf("QueryAllTyped returned error: %v", err)
	}
	if items.Len() != 2 {
		t.Fatalf("unexpected dto count: %d", items.Len())
	}
	first, _ := items.Get(0)
	if first.Username != "alice" {
		t.Fatalf("unexpected first dto: %+v", first)
	}
}

func TestQueryTypedUsesDefaultStructMapper(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc())

	items, err := QueryTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if err != nil {
		t.Fatalf("QueryTyped returned error: %v", err)
	}
	if items.Len() != 2 {
		t.Fatalf("unexpected dto count: %d", items.Len())
	}
	first, _ := items.Get(0)
	if first.Username != "alice" {
		t.Fatalf("unexpected first dto: %+v", first)
	}
}
