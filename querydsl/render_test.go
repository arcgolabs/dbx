package querydsl_test

import (
	"reflect"
	"testing"

	"github.com/arcgolabs/dbx/querydsl"
)

func TestSelectBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	roles := MustSchema("roles", RoleSchema{})

	query := Select(users.ID, users.Username, roles.Name).
		From(users).
		Join(roles).On(users.RoleID.EqColumn(roles.ID)).
		Where(And(users.Status.Eq(1), Like(users.Username, "a%"))).
		OrderBy(users.ID.Desc()).
		Limit(20).
		Offset(10)

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username", "roles"."name" FROM "users" INNER JOIN "roles" ON "users"."role_id" = "roles"."id" WHERE ("users"."status" = ? AND "users"."username" LIKE ?) ORDER BY "users"."id" DESC LIMIT 20 OFFSET 10`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{1, "a%"}) {
		t.Fatalf("unexpected sqlite select args: %#v", bound.Args.Values())
	}
}

func TestSelectFromBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := SelectFrom(users, users.ID, users.Username).
		Where(users.Status.Eq(1)).
		OrderBy(users.ID.Asc())

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ? ORDER BY "users"."id" ASC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite select-from SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{1}) {
		t.Fatalf("unexpected sqlite select-from args: %#v", bound.Args.Values())
	}
}

func TestFromThenSelectBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := From(users).
		Select(users.ID, users.Username).
		Where(users.Status.Eq(1))

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" WHERE "users"."status" = ?`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite from-select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestViewAndColumnBuildSQLite(t *testing.T) {
	activeUsers := View("active_users")
	activeID := querydsl.Col[int64](activeUsers, "id")
	activeUsername := querydsl.Col[string](activeUsers, "username")

	query := SelectFrom(activeUsers, activeID, activeUsername).
		Where(activeID.Gt(int64(10))).
		OrderBy(activeID.Desc())

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "active_users"."id", "active_users"."username" FROM "active_users" WHERE "active_users"."id" > ? ORDER BY "active_users"."id" DESC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite view SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{int64(10)}) {
		t.Fatalf("unexpected sqlite view args: %#v", bound.Args.Values())
	}
}

func TestAllColumnsBuildSQLite(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := Select(AllColumns(users).Values()...).From(users)
	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username", "users"."email_address", "users"."status", "users"."role_id" FROM "users"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected all columns SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestSelectBuildPostgresWithAliasAndIn(t *testing.T) {
	users := Alias(MustSchema("users", UserSchema{}), "u")

	query := Select(users.ID, users.Username).
		From(users).
		Where(users.ID.In(int64(1), int64(2), int64(3))).
		Offset(5)

	bound, err := query.Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "u"."id", "u"."username" FROM "users" AS "u" WHERE "u"."id" IN ($1, $2, $3) OFFSET 5`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected postgres select SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{int64(1), int64(2), int64(3)}) {
		t.Fatalf("unexpected postgres select args: %#v", bound.Args.Values())
	}
}

func TestInsertUpdateDeleteBuildMySQL(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	insertBound, err := InsertInto(users).
		Values(users.Username.Set("alice"), users.Status.Set(1)).
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("insert Build returned error: %v", err)
	}
	insertSQL := "INSERT INTO `users` (`username`, `status`) VALUES (?, ?)"
	if insertBound.SQL != insertSQL {
		t.Fatalf("unexpected mysql insert SQL:\nwant: %s\n got: %s", insertSQL, insertBound.SQL)
	}
	if !reflect.DeepEqual(insertBound.Args.Values(), []any{"alice", 1}) {
		t.Fatalf("unexpected mysql insert args: %#v", insertBound.Args.Values())
	}

	updateBound, err := Update(users).
		Set(users.Status.Set(2)).
		Where(users.ID.Eq(int64(10))).
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("update Build returned error: %v", err)
	}
	updateSQL := "UPDATE `users` SET `status` = ? WHERE `users`.`id` = ?"
	if updateBound.SQL != updateSQL {
		t.Fatalf("unexpected mysql update SQL:\nwant: %s\n got: %s", updateSQL, updateBound.SQL)
	}
	if !reflect.DeepEqual(updateBound.Args.Values(), []any{2, int64(10)}) {
		t.Fatalf("unexpected mysql update args: %#v", updateBound.Args.Values())
	}

	deleteBound, err := DeleteFrom(users).
		Where(users.ID.Eq(int64(10))).
		Build(testMySQLDialect{})
	if err != nil {
		t.Fatalf("delete Build returned error: %v", err)
	}
	deleteSQL := "DELETE FROM `users` WHERE `users`.`id` = ?"
	if deleteBound.SQL != deleteSQL {
		t.Fatalf("unexpected mysql delete SQL:\nwant: %s\n got: %s", deleteSQL, deleteBound.SQL)
	}
	if !reflect.DeepEqual(deleteBound.Args.Values(), []any{int64(10)}) {
		t.Fatalf("unexpected mysql delete args: %#v", deleteBound.Args.Values())
	}
}

func TestJoinRelationBuildSQLite(t *testing.T) {
	users := Alias(MustSchema("users", UserSchema{}), "u")
	roles := Alias(MustSchema("roles", RoleSchema{}), "r")

	query := Select(users.ID, roles.Name).From(users)
	if _, err := JoinRelation(query, users, users.Role, roles); err != nil {
		t.Fatalf("JoinRelation returned error: %v", err)
	}

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "u"."id", "r"."name" FROM "users" AS "u" INNER JOIN "roles" AS "r" ON "u"."role_id" = "r"."id"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite relation join SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestJoinRelationManyToManyBuildSQLite(t *testing.T) {
	users := Alias(MustSchema("users", UserSchema{}), "u")
	roles := Alias(MustSchema("roles", RoleSchema{}), "r")

	query := Select(users.ID, roles.Name).From(users)
	if _, err := JoinRelation(query, users, users.Roles, roles); err != nil {
		t.Fatalf("JoinRelation returned error: %v", err)
	}

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "u"."id", "r"."name" FROM "users" AS "u" INNER JOIN "user_roles" ON "u"."id" = "user_roles"."user_id" INNER JOIN "roles" AS "r" ON "user_roles"."role_id" = "r"."id"`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected sqlite many-to-many relation join SQL:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
}

func TestSelectBuildWithGroupByHavingAndAggregates(t *testing.T) {
	users := MustSchema("users", UserSchema{})

	query := Select(
		users.Status,
		CountAll().As("user_count"),
	).
		From(users).
		WithDistinct().
		GroupBy(users.Status).
		Having(CountAll().Gt(int64(1))).
		OrderBy(users.Status.Asc())

	bound, err := query.Build(testSQLiteDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT DISTINCT "users"."status", COUNT(*) AS "user_count" FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > ? ORDER BY "users"."status" ASC`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected aggregate sql:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{int64(1)}) {
		t.Fatalf("unexpected aggregate args: %#v", bound.Args.Values())
	}
}

func TestSelectBuildWithSubqueryAndExists(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	roles := MustSchema("roles", RoleSchema{})

	subquery := Select(roles.ID).
		From(roles).
		Where(roles.Name.Eq("admin"))

	existsQuery := Select(roles.ID).
		From(roles).
		Where(And(
			roles.ID.EqColumn(users.RoleID),
			roles.Name.Eq("admin"),
		))

	query := Select(users.ID, users.Username).
		From(users).
		Where(And(
			users.RoleID.InQuery(subquery),
			Exists(existsQuery),
		))

	bound, err := query.Build(testPostgresDialect{})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	expectedSQL := `SELECT "users"."id", "users"."username" FROM "users" WHERE ("users"."role_id" IN (SELECT "roles"."id" FROM "roles" WHERE "roles"."name" = $1) AND EXISTS (SELECT "roles"."id" FROM "roles" WHERE ("roles"."id" = "users"."role_id" AND "roles"."name" = $2)))`
	if bound.SQL != expectedSQL {
		t.Fatalf("unexpected subquery sql:\nwant: %s\n got: %s", expectedSQL, bound.SQL)
	}
	if !reflect.DeepEqual(bound.Args.Values(), []any{"admin", "admin"}) {
		t.Fatalf("unexpected subquery args: %#v", bound.Args.Values())
	}
}
