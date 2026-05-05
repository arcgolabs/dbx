package querydsl_test

import (
	"testing"

	"github.com/arcgolabs/dbx/querydsl"
)

type userSummaryRow struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

func TestSelectFromIntoCarriesResultTypeAndBuilds(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := querydsl.SelectFromInto[userSummaryRow](users, users.ID, users.Username).
		Where(users.Status.Eq(1)).
		OrderBy(users.ID.Desc()).
		Limit(10)

	var _ querydsl.Builder = query

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ? ORDER BY "users"."id" DESC LIMIT 10`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestSelectIntoPreservesTypeAcrossJoinBuilder(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	roles := MustSchema("roles", RoleSchema{})

	query := querydsl.SelectInto[userSummaryRow](users.ID, users.Username).
		From(users).
		Join(roles).
		On(users.RoleID.EqColumn(roles.ID)).
		Where(roles.Name.Eq("admin"))

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" INNER JOIN "roles" ON "users"."role_id" = "roles"."id" WHERE "roles"."name" = ?`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed join SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestSelectIntoZeroValueCanBeChained(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	var query querydsl.SelectResult[userSummaryRow]

	bound, err := query.Select(users.ID).From(users).Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id" FROM "users"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected zero-value typed select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}
