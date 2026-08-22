package repository_test

import (
	"context"
	"errors"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/stretchr/testify/require"
)

type SoftUser struct {
	ID      int64  `dbx:"id"`
	Name    string `dbx:"name"`
	Deleted bool   `dbx:"deleted"`
	Version int64  `dbx:"version"`
}

type SoftUserSchema struct {
	schemax.Schema[SoftUser]
	ID      columnx.Column[SoftUser, int64]  `dbx:"id,pk,auto"`
	Name    columnx.Column[SoftUser, string] `dbx:"name"`
	Deleted columnx.Column[SoftUser, bool]   `dbx:"deleted,default=false"`
	Version columnx.Column[SoftUser, int64]  `dbx:"version,default=1"`
}

func TestPatchSoftDeleteAndBatchAPIs(t *testing.T) {
	repo, users, ctx := newSoftUserRepo(t, "file:repository_patch_soft_delete_test?mode=memory&cache=shared")
	require.NoError(t, repo.CreateMany(ctx, &SoftUser{Name: "alice"}, &SoftUser{Name: "bob"}))

	alice, err := repo.FirstSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	key := repository.KeySet(repository.Part(users.ID, alice.ID))
	_, err = repository.PatchSet(repo, key).
		Set(users.Name.Set("alice-v2")).
		Version(users.Version, alice.Version).
		Apply(ctx)
	require.NoError(t, err)

	updated, err := repo.By(users.ID).Get(ctx, alice.ID)
	require.NoError(t, err)
	require.Equal(t, "alice-v2", updated.Name)
	require.EqualValues(t, alice.Version+1, updated.Version)

	_, err = repository.PatchSet(repo, key).
		Set(users.Name.Set("stale")).
		Version(users.Version, alice.Version).
		Apply(ctx)
	require.True(t, errors.Is(err, repository.ErrVersionConflict))

	_, err = repo.SoftDeleteByKeySet(ctx, key)
	require.NoError(t, err)

	visible, err := repo.List(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, visible.Len())
	remaining, ok := visible.GetFirst()
	require.True(t, ok)
	require.Equal(t, "bob", remaining.Name)

	all, err := repository.Query(repo).WithDeleted().List(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, all.Len())

	deleted, err := repository.Query(repo).OnlyDeleted().List(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, deleted.Len())
	deletedUser, ok := deleted.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alice-v2", deletedUser.Name)

	seen := 0
	repository.Query(repo).WithDeleted().Each(ctx)(func(_ SoftUser, err error) bool {
		require.NoError(t, err)
		seen++
		return true
	})
	require.Equal(t, 2, seen)

	batches := 0
	err = repository.Query(repo).WithDeleted().OrderBy(users.ID.Asc()).Batch(ctx, 1, func(items *collectionx.List[SoftUser]) error {
		require.Equal(t, 1, items.Len())
		batches++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, batches)
}

func newSoftUserRepo(t *testing.T, dsn string) (*repository.Base[SoftUser, SoftUserSchema], SoftUserSchema, context.Context) {
	t.Helper()

	ctx := context.Background()
	core := openRepositoryCore(t, dsn)
	users := schemax.MustSchema("soft_users", SoftUserSchema{})
	mustAutoMigrate(ctx, t, core, users)
	return repository.NewWithOptions[SoftUser](core, users, repository.WithSoftDeleteFlag(users.Deleted, false, true)), users, ctx
}
