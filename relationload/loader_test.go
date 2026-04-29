package relationload_test

import (
	"context"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	relationload "github.com/arcgolabs/dbx/relationload"
	"github.com/samber/mo"
)

func TestLoaderBelongsToBindsRepeatedDependencies(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, relationTestSchemaDDL,
		`INSERT INTO "roles" ("id","name") VALUES (2,'admin')`,
	)
	defer cleanup()

	users := MustSchema("users", relationUserSchema{})
	roles := MustSchema("roles", relationRoleSchema{})
	items := collectionx.NewList[relationUser](
		relationUser{ID: 1, Name: "alice", RoleID: 2},
		relationUser{ID: 2, Name: "bob", RoleID: 4},
	)
	loaded := make([]mo.Option[relationRole], items.Len())
	loader := relationload.New[relationUser, relationRole](New(sqlDB, testSQLiteDialect{}), users, roles)

	err := loader.BelongsTo(
		context.Background(),
		items,
		users.Role,
		func(index int, user relationUser, value mo.Option[relationRole]) relationUser {
			loaded[index] = value
			return user
		},
	)
	if err != nil {
		t.Fatalf("BelongsTo returned error: %v", err)
	}
	role, ok := loaded[0].Get()
	if !ok || role.Name != "admin" {
		t.Fatalf("expected first role to be loaded, got %#v", loaded[0])
	}
	if loaded[1].IsPresent() {
		t.Fatalf("expected second role to be absent, got %#v", loaded[1])
	}
}
