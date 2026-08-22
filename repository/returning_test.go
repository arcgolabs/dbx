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

	created, err := repo.CreateReturning(ctx, &User{Name: "alice"})
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	require.Equal(t, "alice", created.Name)
}

func TestCreateReturningIntoScansProjection(t *testing.T) {
	repo, users, ctx := newUserRepo(t, "file:repository_create_returning_projection_test?mode=memory&cache=shared")

	created, err := repo.CreateReturningInto[userNameRow](ctx, &User{Name: "alice"}, users.Name)
	require.NoError(t, err)
	require.Equal(t, "alice", created.Name)
}

func TestUpsertReturningScansEntity(t *testing.T) {
	repo, devices, ctx := newDeviceRepo(t, "file:repository_upsert_returning_test?mode=memory&cache=shared")

	created, err := repo.UpsertReturning(ctx, &Device{DeviceID: "dev-1", Name: "sensor"})
	require.NoError(t, err)
	require.Equal(t, "dev-1", created.DeviceID)
	require.Equal(t, "sensor", created.Name)

	updated, err := repo.UpsertReturning(ctx, &Device{DeviceID: "dev-1", Name: "sensor-v2"})
	require.NoError(t, err)
	require.Equal(t, "dev-1", updated.DeviceID)
	require.Equal(t, "sensor-v2", updated.Name)

	stored, err := repo.By(devices.DeviceID).Get(ctx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor-v2", stored.Name)
}

func TestUpdateReturningScansRowsWithoutMutatingQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_update_returning_test?mode=memory&cache=shared", "alice")
	alice, err := repo.FirstSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	update := querydsl.Update(users).
		Set(users.Name.Set("alice-v2")).
		Where(users.ID.Eq(alice.ID))

	rows, err := repo.UpdateReturning(ctx, update)
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

	deleted, err := repo.DeleteReturningInto[userNameRow](
		ctx,
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

func TestUpdateByVersionReturningScansUpdatedEntity(t *testing.T) {
	repo, users, ctx := newVersionedUserRepo(t, "file:repository_version_returning_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &VersionedUser{Name: "alice", Version: 1}))

	item, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users))
	require.NoError(t, err)

	updated, err := repo.UpdateByVersionReturning(ctx, repository.Key{"id": item.ID}, 1, users.Name.Set("alice-v2"))
	require.NoError(t, err)
	require.Equal(t, item.ID, updated.ID)
	require.Equal(t, "alice-v2", updated.Name)
	require.EqualValues(t, 2, updated.Version)

	_, err = repo.UpdateByVersionReturning(ctx, repository.Key{"id": item.ID}, 1, users.Name.Set("alice-stale"))
	require.ErrorIs(t, err, repository.ErrVersionConflict)
}

func TestUpdateByVersionSetReturningScansUpdatedEntity(t *testing.T) {
	repo, users, ctx := newVersionedUserRepo(t, "file:repository_typed_version_returning_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &VersionedUser{Name: "alice", Version: 1}))

	item, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users))
	require.NoError(t, err)

	key := repository.KeySet(repository.Part(users.ID, item.ID))
	updated, err := repo.UpdateByVersionSetReturning(ctx, key, users.Version, 1, users.Name.Set("alice-v2"))
	require.NoError(t, err)
	require.Equal(t, item.ID, updated.ID)
	require.Equal(t, "alice-v2", updated.Name)
	require.EqualValues(t, 2, updated.Version)

	_, err = repo.UpdateByVersionSetReturning(ctx, key, users.Version, 1, users.Name.Set("alice-stale"))
	require.ErrorIs(t, err, repository.ErrVersionConflict)
}
