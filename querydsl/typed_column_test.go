package querydsl_test

import (
	"testing"

	"github.com/arcgolabs/dbx/querydsl"
)

func TestTypedColumnComparisonsBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	activeUsers := View("active_users")
	activeID := querydsl.Col[int64](activeUsers, "id")
	activeStatus := querydsl.Col[int](activeUsers, "status")

	query := SelectFrom(activeUsers, activeID).
		Where(And(
			activeID.EqColumn(users.ID),
			users.Status.LeColumn(activeStatus),
		))

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "active_users"."id" FROM "active_users" WHERE ("active_users"."id" = "users"."id" AND "users"."status" <= "active_users"."status")`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed column comparison SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestTypedAggregatesBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := Select(
		querydsl.Sum(users.ID).As("sum_id"),
		querydsl.Avg(users.Status).As("avg_status"),
		querydsl.Min(users.Username).As("first_username"),
		querydsl.Max(querydsl.Col[int64](users, "role_id")).As("max_role_id"),
	).From(users)

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT SUM("users"."id") AS "sum_id", AVG("users"."status") AS "avg_status", MIN("users"."username") AS "first_username", MAX("users"."role_id") AS "max_role_id" FROM "users"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed aggregate SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestTypedLikeBuildSQLite(t *testing.T) {
	activeUsers := View("active_users")
	activeUsername := querydsl.Col[string](activeUsers, "username")

	query := SelectFrom(activeUsers, activeUsername).
		Where(querydsl.Like(activeUsername, "a%"))

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "active_users"."username" FROM "active_users" WHERE "active_users"."username" LIKE ?`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed like SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}
