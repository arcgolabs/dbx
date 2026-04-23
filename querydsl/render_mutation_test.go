package querydsl_test

import (
	"github.com/arcgolabs/dbx/querydsl"
	"reflect"
	"testing"

	"github.com/arcgolabs/collectionx"
)

func TestInsertBuildWithMultipleRows(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	bound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		Values(users.Username.Set("bob"), users.Status.Set(2)).
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := "INSERT INTO `users` (`username`, `status`) VALUES (?, ?), (?, ?)"
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected batch insert SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{"alice", 1, "bob", 2}) {
		t.Fatalf("unexpected batch insert args: %#v", bound.Args.Values())
	}
}

func TestInsertBuildWithValuesRowsList(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	rows := collectionx.NewList[collectionx.List[querydsl.Assignment]](
		collectionx.NewList[querydsl.Assignment](
			users.Username.Set("alice"),
			users.Status.Set(1),
		),
		collectionx.NewList[querydsl.Assignment](
			users.Status.Set(2),
			users.Username.Set("bob"),
		),
	)
	bound, err := InsertInto(users).ValuesRowsList(rows).Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := "INSERT INTO `users` (`username`, `status`) VALUES (?, ?), (?, ?)"
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected rows-list insert SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{"alice", 1, "bob", 2}) {
		t.Fatalf("unexpected rows-list insert args: %#v", bound.Args.Values())
	}
}

func TestInsertBuildFromSelect(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	source := Select(users.Username, users.Status).
		From(users).
		Where(users.Status.Eq(1))

	bound, err := InsertInto(users).
		Columns(users.Username, users.Status).
		FromSelect(source).
		Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `INSERT INTO "users" ("username", "status") SELECT "users"."username", "users"."status" FROM "users" WHERE "users"."status" = $1`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected insert-select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{1}) {
		t.Fatalf("unexpected insert-select args: %#v", bound.Args.Values())
	}
}

func TestInsertBuildWithPostgresUpsert(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	bound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		OnConflict(users.Username).
		DoUpdateSet(users.Status.SetExcluded()).
		Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `INSERT INTO "users" ("username", "status") VALUES ($1, $2) ON CONFLICT ("username") DO UPDATE SET "status" = EXCLUDED."status"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected postgres upsert SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{"alice", 1}) {
		t.Fatalf("unexpected postgres upsert args: %#v", bound.Args.Values())
	}
}

func TestInsertBuildWithMySQLUpsert(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	bound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		OnConflict(users.Username).
		DoUpdateSet(users.Status.SetExcluded()).
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := "INSERT INTO `users` (`username`, `status`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `status` = VALUES(`status`)"
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected mysql upsert SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{"alice", 1}) {
		t.Fatalf("unexpected mysql upsert args: %#v", bound.Args.Values())
	}
}

func TestInsertBuildWithMySQLDoNothing(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	bound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		OnConflict(users.Username).
		DoNothing().
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := "INSERT IGNORE INTO `users` (`username`, `status`) VALUES (?, ?)"
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected mysql do-nothing SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestMutationBuildWithReturning(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	insertBound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		Returning(users.ID, users.Username).
		Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("insert Build returned error: %v", err)
	}
	expectedInsertSQL := `INSERT INTO "users" ("username", "status") VALUES ($1, $2) RETURNING "users"."id", "users"."username"`
	if insertBound.SQL != expectedInsertSQL {
		t.Fatalf("unexpected insert returning SQL:\nwant: %s\n got: %s", expectedInsertSQL, insertBound.SQL)
	}

	updateBound, err := Update(users).
		Set(users.Status.Set(2)).
		Where(users.ID.Eq(int64(10))).
		Returning(users.ID, users.Status).
		Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("update Build returned error: %v", err)
	}
	expectedUpdateSQL := `UPDATE "users" SET "status" = ? WHERE "users"."id" = ? RETURNING "users"."id", "users"."status"`
	if updateBound.SQL != expectedUpdateSQL {
		t.Fatalf("unexpected update returning SQL:\nwant: %s\n got: %s", expectedUpdateSQL, updateBound.SQL)
	}

	deleteBound, err := DeleteFrom(users).
		Where(users.ID.Eq(int64(10))).
		Returning(users.ID).
		Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("delete Build returned error: %v", err)
	}
	expectedDeleteSQL := `DELETE FROM "users" WHERE "users"."id" = $1 RETURNING "users"."id"`
	if deleteBound.SQL != expectedDeleteSQL {
		t.Fatalf("unexpected delete returning SQL:\nwant: %s\n got: %s", expectedDeleteSQL, deleteBound.SQL)
	}
}

func TestMutationBuildWithUnsupportedReturning(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	_, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		Returning(users.ID).
		Build(testMySQLDialect{})
	if err == nil {
		t.Fatal("expected error for mysql returning, got nil")
	}
}

func TestSelectBuildWithCTE(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	activeUsers := NamedTable("active_users")
	activeID := NamedColumn[int64](activeUsers, "id")
	activeUsername := NamedColumn[string](activeUsers, "username")

	query := Select(activeID, activeUsername).
		With("active_users", Select(users.ID, users.Username).From(users).Where(users.Status.Eq(1))).
		From(activeUsers).
		OrderBy(activeID.Asc())

	bound, err := query.Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `WITH "active_users" AS (SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = $1) SELECT "active_users"."id", "active_users"."username" FROM "active_users" ORDER BY "active_users"."id" ASC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected cte sql:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{1}) {
		t.Fatalf("unexpected cte args: %#v", bound.Args.Values())
	}
}

func TestSelectBuildWithUnionAllAndOuterOrder(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	roles := MustSchema("roles", RoleSchema{})
	label := ResultColumn[string]("label")

	query := Select(users.Username.As("label")).
		From(users).
		Where(users.Status.Eq(1)).
		UnionAll(
			Select(roles.Name.As("label")).
				From(roles),
		).
		OrderBy(label.Asc()).
		Limit(5)

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."username" AS "label" FROM "users" WHERE "users"."status" = ? UNION ALL SELECT "roles"."name" AS "label" FROM "roles" ORDER BY "label" ASC LIMIT 5`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected union sql:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{1}) {
		t.Fatalf("unexpected union args: %#v", bound.Args.Values())
	}
}

func TestSelectBuildWithCaseWhen(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	statusLabel := CaseWhen[string](users.Status.Eq(1), "active").
		When(users.Status.Eq(2), "blocked").
		Else("unknown")

	query := Select(
		users.ID,
		statusLabel.As("status_label"),
	).
		From(users).
		Where(statusLabel.Ne("unknown")).
		OrderBy(statusLabel.Asc())

	bound, err := query.Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", CASE WHEN "users"."status" = $1 THEN $2 WHEN "users"."status" = $3 THEN $4 ELSE $5 END AS "status_label" FROM "users" WHERE CASE WHEN "users"."status" = $6 THEN $7 WHEN "users"."status" = $8 THEN $9 ELSE $10 END <> $11 ORDER BY CASE WHEN "users"."status" = $12 THEN $13 WHEN "users"."status" = $14 THEN $15 ELSE $16 END ASC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected case sql:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	expectedArgs := []any{1, "active", 2, "blocked", "unknown", 1, "active", 2, "blocked", "unknown", "unknown", 1, "active", 2, "blocked", "unknown"}
	if !reflect.DeepEqual(bound.Args.Values(), expectedArgs) {
		t.Fatalf("unexpected case args: %#v", bound.Args.Values())
	}
}
