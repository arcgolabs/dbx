package repository_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/arcgolabs/dbx"
	sqlitedialect "github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/stretchr/testify/require"
)

func TestBaseExistsUsesSingleColumnLimit(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", "file:repository_exists_optimized_test?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("close sqlite: %v", closeErr)
		}
	})

	var querySQL string
	core := dbx.MustNewWithOptions(raw, sqlitedialect.New(), dbx.WithHooks(dbx.HookFuncs{
		AfterFunc: func(_ context.Context, actual *dbx.HookEvent) {
			if actual != nil && actual.Operation == dbx.OperationQuery {
				querySQL = actual.SQL
			}
		},
	}))
	users := schemax.MustSchema("users", UserSchema{})
	mustAutoMigrate(ctx, t, core, users)
	repo := repository.New[User](core, users)
	seedUsers(ctx, t, repo, "alice", "bob")

	exists, err := repo.Exists(ctx, querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("alice")).OrderBy(users.Name.Asc()))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, `SELECT "users"."id" FROM "users" WHERE "users"."name" = ? LIMIT 1`, querySQL)
}
