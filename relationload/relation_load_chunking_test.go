package relationload_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/samber/mo"
)

func TestLoadBelongsToChunksLargeINQueries(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, relationTestSchemaDDL)
	defer cleanup()

	for i := 1; i <= 950; i++ {
		if _, err := sqlDB.ExecContext(context.Background(), `INSERT INTO "roles" ("id","name") VALUES (?, ?)`, i, fmt.Sprintf("role-%d", i)); err != nil {
			t.Fatalf("insert role %d: %v", i, err)
		}
	}

	users := MustSchema("users", relationUserSchema{})
	roles := MustSchema("roles", relationRoleSchema{})
	items := make([]relationUser, 0, 950)
	for i := 1; i <= 950; i++ {
		items = append(items, relationUser{ID: int64(i), Name: fmt.Sprintf("user-%d", i), RoleID: int64(i)})
	}
	loaded := make([]mo.Option[relationRole], len(items))

	err := LoadBelongsTo(
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		items,
		users,
		MustMapper[relationUser](users),
		users.Role,
		roles,
		MustMapper[relationRole](roles),
		func(index int, _ *relationUser, value mo.Option[relationRole]) {
			loaded[index] = value
		},
	)
	if err != nil {
		t.Fatalf("LoadBelongsTo returned error: %v", err)
	}
	last, ok := loaded[len(loaded)-1].Get()
	if !ok || last.Name != "role-950" {
		t.Fatalf("expected final role to load after chunking, got ok=%v value=%+v", ok, last)
	}
}

func TestLoadManyToManyChunksLargePairQueries(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, relationTestSchemaDDL)
	defer cleanup()

	seedSharedManyToManyRows(t, sqlDB, 950)

	users := MustSchema("users", relationUserSchema{})
	tags := MustSchema("tags", relationTagSchema{})
	items := chunkedRelationUsers(950)
	loaded := make([][]relationTag, len(items))

	err := LoadManyToMany(
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		items,
		users,
		MustMapper[relationUser](users),
		users.Tags,
		tags,
		MustMapper[relationTag](tags),
		func(index int, _ *relationUser, value []relationTag) {
			loaded[index] = value
		},
	)
	if err != nil {
		t.Fatalf("LoadManyToMany returned error: %v", err)
	}
	assertSharedRelationTags(t, loaded[0], "first")
	assertSharedRelationTags(t, loaded[len(loaded)-1], "final")
}

func TestLoadHasManyReturnsDeterministicOrder(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, relationTestSchemaDDL,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("id","name","role_id") VALUES (1,'alice',1)`,
		`INSERT INTO "posts" ("id","user_id","title") VALUES (200,1,'second'),(100,1,'first')`,
	)
	defer cleanup()

	users := MustSchema("users", relationUserSchema{})
	posts := MustSchema("posts", relationPostSchema{})
	items := []relationUser{{ID: 1, Name: "alice"}}
	loaded := make([][]relationPost, len(items))

	err := LoadHasMany(
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		items,
		users,
		MustMapper[relationUser](users),
		users.Posts,
		posts,
		MustMapper[relationPost](posts),
		func(index int, _ *relationUser, value []relationPost) {
			loaded[index] = value
		},
	)
	if err != nil {
		t.Fatalf("LoadHasMany returned error: %v", err)
	}
	if len(loaded[0]) != 2 || loaded[0][0].ID != 100 || loaded[0][1].ID != 200 {
		t.Fatalf("expected deterministic has-many order by primary key, got %+v", loaded[0])
	}
}

func TestLoadHasOneRejectsDuplicateTargets(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, relationTestSchemaDDL,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("id","name","role_id") VALUES (1,'alice',1)`,
		`INSERT INTO "profiles" ("id","user_id","bio") VALUES (10,1,'one'),(11,1,'two')`,
	)
	defer cleanup()

	users := MustSchema("users", relationUserSchema{})
	profiles := MustSchema("profiles", relationProfileSchema{})
	items := []relationUser{{ID: 1, Name: "alice"}}

	err := LoadHasOne(
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		items,
		users,
		MustMapper[relationUser](users),
		users.Profile,
		profiles,
		MustMapper[relationProfile](profiles),
		func(int, *relationUser, mo.Option[relationProfile]) {},
	)
	if !errors.Is(err, ErrRelationCardinality) {
		t.Fatalf("expected relation cardinality error, got: %v", err)
	}
}

func seedSharedManyToManyRows(t *testing.T, sqlDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, count int) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), `INSERT INTO "roles" ("id","name") VALUES (1,'r')`); err != nil {
		t.Fatalf("insert role: %v", err)
	}
	if _, err := sqlDB.ExecContext(context.Background(), `INSERT INTO "tags" ("id","name") VALUES (1,'shared')`); err != nil {
		t.Fatalf("insert tag: %v", err)
	}
	for i := 1; i <= count; i++ {
		insertSharedUserAndPair(t, sqlDB, i)
	}
}

func insertSharedUserAndPair(t *testing.T, sqlDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, index int) {
	t.Helper()
	if _, err := sqlDB.ExecContext(context.Background(), `INSERT INTO "users" ("id","name","role_id") VALUES (?,?,1)`, index, fmt.Sprintf("user-%d", index)); err != nil {
		t.Fatalf("insert user %d: %v", index, err)
	}
	if _, err := sqlDB.ExecContext(context.Background(), `INSERT INTO "user_tags" ("user_id","tag_id") VALUES (?,1)`, index); err != nil {
		t.Fatalf("insert pair %d: %v", index, err)
	}
}

func chunkedRelationUsers(count int) []relationUser {
	items := make([]relationUser, 0, count)
	for i := 1; i <= count; i++ {
		items = append(items, relationUser{ID: int64(i), Name: fmt.Sprintf("user-%d", i)})
	}
	return items
}

func assertSharedRelationTags(t *testing.T, tags []relationTag, label string) {
	t.Helper()
	if len(tags) != 1 || tags[0].Name != "shared" {
		t.Fatalf("unexpected %s many-to-many payload: %+v", label, tags)
	}
}
