package repository_test

import (
	"testing"

	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

type userNameRow struct {
	Name string `dbx:"name"`
}

func TestCreateReturningScansEntity(t *testing.T) {
	repo, _, ctx := newUserRepo(t, "file:repository_create_returning_test?mode=memory&cache=shared")

	created, err := repository.CreateReturning(ctx, repo, &User{Name: "alice"})
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	require.Equal(t, "alice", created.Name)
}

func TestCreateReturningIntoScansProjection(t *testing.T) {
	repo, users, ctx := newUserRepo(t, "file:repository_create_returning_projection_test?mode=memory&cache=shared")

	created, err := repository.CreateReturningInto[userNameRow](ctx, repo, &User{Name: "alice"}, users.Name)
	require.NoError(t, err)
	require.Equal(t, "alice", created.Name)
}

func TestUpdateReturningScansRowsWithoutMutatingQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_update_returning_test?mode=memory&cache=shared", "alice")
	alice, err := repo.FirstSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	update := querydsl.Update(users).
		Set(users.Name.Set("alice-v2")).
		Where(users.ID.Eq(alice.ID))

	rows, err := repository.UpdateReturning(ctx, repo, update)
	require.NoError(t, err)
	require.Equal(t, 1, rows.Len())
	updated, ok := rows.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alice-v2", updated.Name)
	require.Equal(t, 0, update.ReturningItems.Len())
}

func TestDeleteReturningIntoScansProjection(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_delete_returning_test?mode=memory&cache=shared", "alice", "bob")
	alice, err := repo.FirstSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	deleted, err := repository.DeleteReturningInto[userNameRow](
		ctx,
		repo,
		querydsl.DeleteFrom(users).Where(users.ID.Eq(alice.ID)),
		users.Name,
	)
	require.NoError(t, err)
	require.Equal(t, 1, deleted.Len())
	row, ok := deleted.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alice", row.Name)

	total, err := repo.Count(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
}
