package querydsl_test

import (
	"testing"

	"github.com/arcgolabs/dbx/querydsl"
)

type activeUsersSource struct {
	querydsl.Table
	ID       querydsl.Column[int64]  `dbx:"id"`
	Username querydsl.Column[string] `dbx:"username"`
	Status   querydsl.Column[int]
	Ignored  querydsl.Column[string] `dbx:"-"`
}

func TestMustSourceBindsTypedColumns(t *testing.T) {
	activeUsers := querydsl.MustSource("active_users", activeUsersSource{})

	query := SelectFrom(activeUsers, activeUsers.ID, activeUsers.Username).
		Where(And(
			activeUsers.ID.Gt(100),
			querydsl.Like(activeUsers.Username, "a%"),
		)).
		OrderBy(activeUsers.Status.Desc())

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "active_users"."id", "active_users"."username" FROM "active_users" WHERE ("active_users"."id" > ? AND "active_users"."username" LIKE ?) ORDER BY "active_users"."status" DESC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected source SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if activeUsers.Ignored.Name() != "" {
		t.Fatalf("ignored column was bound: %q", activeUsers.Ignored.Name())
	}
}

func TestMustSourceAsBindsAlias(t *testing.T) {
	activeUsers := querydsl.MustSourceAs("active_users", "au", activeUsersSource{})

	query := SelectFrom(activeUsers, activeUsers.ID).
		Where(activeUsers.ID.Eq(1))

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "au"."id" FROM "active_users" AS "au" WHERE "au"."id" = ?`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected aliased source SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}
