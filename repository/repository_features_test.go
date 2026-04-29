package repository_test

import (
	"context"
	"testing"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/stretchr/testify/require"
)

func TestBaseByIDNotFoundAsErrorOption(t *testing.T) {
	ctx := context.Background()
	core := openRepositoryCore(t, "file:repository_not_found_option_test?mode=memory&cache=shared")
	users := schemax.MustSchema("users", UserSchema{})
	mustAutoMigrate(ctx, t, core, users)

	defaultRepo := repository.New[User](core, users)
	_, err := defaultRepo.DeleteByID(ctx, int64(404))
	require.NoError(t, err)
	_, err = defaultRepo.UpdateByID(ctx, int64(404), users.Name.Set("missing"))
	require.NoError(t, err)

	strictRepo := repository.NewWithOptions[User](core, users, repository.WithByIDNotFoundAsError(true))
	_, err = strictRepo.DeleteByID(ctx, int64(404))
	require.ErrorIs(t, err, repository.ErrNotFound)
	_, err = strictRepo.UpdateByID(ctx, int64(404), users.Name.Set("missing"))
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestBaseCreateManyAndUpsert(t *testing.T) {
	userRepo, _, userCtx := newUserRepo(t, "file:repository_create_many_users_test?mode=memory&cache=shared")
	require.NoError(t, userRepo.CreateMany(userCtx, &User{Name: "alice"}, &User{Name: "bob"}))

	total, err := userRepo.Count(userCtx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)

	deviceRepo, _, deviceCtx := newDeviceRepo(t, "file:repository_upsert_devices_test?mode=memory&cache=shared")
	require.NoError(t, deviceRepo.Create(deviceCtx, &Device{DeviceID: "dev-1", Name: "sensor"}))
	require.NoError(t, deviceRepo.Upsert(deviceCtx, &Device{DeviceID: "dev-1", Name: "sensor-v2"}))

	device, err := deviceRepo.GetByID(deviceCtx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor-v2", device.Name)
}

func TestBaseCompositePrimaryKeyByKey(t *testing.T) {
	repo, memberships, ctx := newMembershipRepo(t, "file:repository_composite_key_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &Membership{TenantID: 100, UserID: 200, Role: "viewer"}))

	key := repository.Key{"tenant_id": int64(100), "user_id": int64(200)}

	item, err := repo.GetByKey(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "viewer", item.Role)

	_, err = repo.UpdateByKey(ctx, key, memberships.Role.Set("admin"))
	require.NoError(t, err)

	updated, err := repo.GetByKey(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "admin", updated.Role)

	_, err = repo.DeleteByKey(ctx, key)
	require.NoError(t, err)

	_, err = repo.GetByKey(ctx, key)
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestBaseSpecAPIs(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_spec_test?mode=memory&cache=shared", "alice", "bob")

	items, err := repo.ListSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.Equal(t, 1, items.Len())

	exists, err := repo.ExistsSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.True(t, exists)

	total, err := repo.CountSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.EqualValues(t, 1, total)

	page, err := repo.ListPageSpec(ctx, 1, 1, repository.OrderBy(users.Name.Asc()))
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Equal(t, 1, page.Items.Len())
}

func TestBaseOptionAPIs(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_option_api_test?mode=memory&cache=shared", "alice")

	noneByID, err := repo.GetByIDOption(ctx, int64(99999))
	require.NoError(t, err)
	require.False(t, noneByID.IsPresent())

	someBySpec, err := repo.FirstSpecOption(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	item, ok := someBySpec.Get()
	require.True(t, ok)
	require.Equal(t, "alice", item.Name)

	noneBySpec, err := repo.FirstSpecOption(ctx, repository.Where(users.Name.Eq("nobody")))
	require.NoError(t, err)
	require.False(t, noneBySpec.IsPresent())
}

func TestTypedKeyAPIs(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_typed_key_api_test?mode=memory&cache=shared", "alice", "bob")
	alice, err := repo.FirstSpec(ctx, repository.Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	byID := repository.By(repo, users.ID)
	exists, err := byID.Exists(ctx, alice.ID)
	require.NoError(t, err)
	require.True(t, exists)

	got, err := byID.Get(ctx, alice.ID)
	require.NoError(t, err)
	require.Equal(t, "alice", got.Name)

	optional, err := byID.GetOption(ctx, int64(404))
	require.NoError(t, err)
	require.False(t, optional.IsPresent())

	_, err = byID.Update(ctx, alice.ID, users.Name.Set("alice-v2"))
	require.NoError(t, err)
	updated, err := byID.Get(ctx, alice.ID)
	require.NoError(t, err)
	require.Equal(t, "alice-v2", updated.Name)

	_, err = byID.Delete(ctx, alice.ID)
	require.NoError(t, err)
	exists, err = byID.Exists(ctx, alice.ID)
	require.NoError(t, err)
	require.False(t, exists)

	byName := repository.By(repo, users.Name)
	bob, err := byName.Get(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, "bob", bob.Name)
}

func TestTypedKeyNilRepository(t *testing.T) {
	users := schemax.MustSchema("users", UserSchema{})
	byID := repository.By((*repository.Base[User, UserSchema])(nil), users.ID)

	_, err := byID.Get(context.Background(), int64(1))
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = byID.Exists(context.Background(), int64(1))
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = byID.Update(context.Background(), int64(1), users.Name.Set("alice"))
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = byID.Delete(context.Background(), int64(1))
	require.ErrorIs(t, err, dbx.ErrNilDB)
}

func TestBaseUpdateByVersion(t *testing.T) {
	repo, users, ctx := newVersionedUserRepo(t, "file:repository_version_conflict_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &VersionedUser{Name: "alice", Version: 1}))

	item, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users))
	require.NoError(t, err)

	key := repository.Key{"id": item.ID}
	_, err = repo.UpdateByVersion(ctx, key, 1, users.Name.Set("alice-v2"))
	require.NoError(t, err)

	_, err = repo.UpdateByVersion(ctx, key, 1, users.Name.Set("alice-stale"))
	require.ErrorIs(t, err, repository.ErrVersionConflict)
}
