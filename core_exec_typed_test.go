package dbx_test

import (
	"context"
	"database/sql"
	"errors"
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

func TestGetTypedReturnsOneRow(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		Where(users.Username.Eq("alice"))

	item, err := GetTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if err != nil {
		t.Fatalf("GetTyped returned error: %v", err)
	}
	if item.Username != "alice" {
		t.Fatalf("unexpected item: %+v", item)
	}
}

func TestGetTypedRequiresOneRow(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		Where(users.Username.Eq("missing"))

	_, err := GetTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestFindTypedReturnsNone(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		Where(users.Username.Eq("missing"))

	item, err := FindTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if err != nil {
		t.Fatalf("FindTyped returned error: %v", err)
	}
	if item.IsPresent() {
		t.Fatalf("expected none, got %v", item.MustGet())
	}
}

func TestFindTypedRejectsTooManyRows(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc())

	_, err := FindTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("expected ErrTooManyRows, got %v", err)
	}
}

func TestQueryOneTypedUsesExplicitMapper(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	mapper := MustMapper[UserSummary](users)
	query := SelectInto[UserSummary](users.ID, users.Username).
		From(users).
		Where(users.Username.Eq("alice"))

	item, err := QueryOneTyped[UserSummary](context.Background(), New(sqlDB, testSQLiteDialect{}), query, mapper)
	if err != nil {
		t.Fatalf("QueryOneTyped returned error: %v", err)
	}
	if item.Username != "alice" {
		t.Fatalf("unexpected item: %+v", item)
	}
}

func TestQueryScalarReturnsValue(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectValue(users.ID).
		From(users).
		Where(users.Username.Eq("alice"))

	id, err := QueryScalar[int64](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if err != nil {
		t.Fatalf("QueryScalar returned error: %v", err)
	}
	if id != 1 {
		t.Fatalf("unexpected id: %d", id)
	}
}

func TestQueryScalarOptionReturnsNone(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectValue(users.ID).
		From(users).
		Where(users.Username.Eq("missing"))

	id, err := QueryScalarOption[int64](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if err != nil {
		t.Fatalf("QueryScalarOption returned error: %v", err)
	}
	if id.IsPresent() {
		t.Fatalf("expected none, got %d", id.MustGet())
	}
}

func TestQueryScalarRejectsTooManyRows(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('alice','a@x.com',1,1),('bob','b@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	query := SelectValue(users.ID).
		From(users).
		OrderBy(users.ID.Asc())

	_, err := QueryScalar[int64](context.Background(), New(sqlDB, testSQLiteDialect{}), query)
	if !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("expected ErrTooManyRows, got %v", err)
	}
}
