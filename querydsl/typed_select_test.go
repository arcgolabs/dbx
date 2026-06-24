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

func TestSelectValueAcceptsTypedAliases(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	countQuery := querydsl.SelectValue(querydsl.CountAll().As("total")).
		From(users)
	countBound, err := countQuery.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build count query returned error: %v", err)
	}
	expectedCountSQL := `SELECT COUNT(*) AS "total" FROM "users"`
	if countBound.SQL != expectedCountSQL {
		t.Fatalf("unexpected typed aggregate alias SQL:\nwant: %s\n got: %s", expectedCountSQL, countBound.SQL)
	}

	nameQuery := querydsl.SelectValue(users.Username.As("name")).
		From(users)
	nameBound, err := nameQuery.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build name query returned error: %v", err)
	}
	expectedNameSQL := `SELECT "users"."username" AS "name" FROM "users"`
	if nameBound.SQL != expectedNameSQL {
		t.Fatalf("unexpected typed column alias SQL:\nwant: %s\n got: %s", expectedNameSQL, nameBound.SQL)
	}

	activeUsers := View("active_users")
	activeID := querydsl.Col[int64](activeUsers, "id")
	idQuery := querydsl.SelectValue(activeID.As("active_id")).
		From(activeUsers)
	idBound, err := idQuery.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build ad-hoc id query returned error: %v", err)
	}
	expectedIDSQL := `SELECT "active_users"."id" AS "active_id" FROM "active_users"`
	if idBound.SQL != expectedIDSQL {
		t.Fatalf("unexpected typed ad-hoc column alias SQL:\nwant: %s\n got: %s", expectedIDSQL, idBound.SQL)
	}
}

func TestSelectResultWithTypedCTE(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	activeUsers := View("active_users")
	activeID := querydsl.Col[int64](activeUsers, "id")
	activeUsername := querydsl.Col[string](activeUsers, "username")

	cte := querydsl.SelectFromInto[userSummaryRow](users, users.ID, users.Username).
		Where(users.Status.Eq(1))
	query := querydsl.SelectFromInto[userSummaryRow](activeUsers, activeID, activeUsername).
		WithTyped("active_users", cte)

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `WITH "active_users" AS (SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ?) SELECT "active_users"."id", "active_users"."username" FROM "active_users"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed CTE SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestSelectResultTypedUnion(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	active := querydsl.SelectFromInto[userSummaryRow](users, users.ID, users.Username).
		Where(users.Status.Eq(1))
	blocked := querydsl.SelectFromInto[userSummaryRow](users, users.ID, users.Username).
		Where(users.Status.Eq(2))

	query := active.UnionAllTyped(blocked).
		OrderBy(users.ID.Asc())

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ? UNION ALL SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ? ORDER BY "users"."id" ASC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected typed union SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}
