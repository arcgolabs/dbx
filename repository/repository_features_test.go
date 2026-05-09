package repository_test

import (
	"context"
	"testing"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/stretchr/testify/require"
)

type typedUserNameRow struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

func TestBaseTypedKeyNotFoundAsErrorOption(t *testing.T) {
	ctx := context.Background()
	core := openRepositoryCore(t, "file:repository_not_found_option_test?mode=memory&cache=shared")
	users := schemax.MustSchema("users", UserSchema{})
	mustAutoMigrate(ctx, t, core, users)

	defaultRepo := repository.New[User](core, users)
	defaultByID := repository.By(defaultRepo, users.ID)
	_, err := defaultByID.Delete(ctx, int64(404))
	require.NoError(t, err)
	_, err = defaultByID.Update(ctx, int64(404), users.Name.Set("missing"))
	require.NoError(t, err)

	strictRepo := repository.NewWithOptions[User](core, users, repository.WithKeyNotFoundAsError(true))
	strictByID := repository.By(strictRepo, users.ID)
	_, err = strictByID.Delete(ctx, int64(404))
	require.ErrorIs(t, err, repository.ErrNotFound)
	_, err = strictByID.Update(ctx, int64(404), users.Name.Set("missing"))
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestBaseCreateManyAndUpsert(t *testing.T) {
	userRepo, _, userCtx := newUserRepo(t, "file:repository_create_many_users_test?mode=memory&cache=shared")
	require.NoError(t, userRepo.CreateMany(userCtx, &User{Name: "alice"}, &User{Name: "bob"}))

	total, err := userRepo.Count(userCtx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)

	deviceRepo, devices, deviceCtx := newDeviceRepo(t, "file:repository_upsert_devices_test?mode=memory&cache=shared")
	require.NoError(t, deviceRepo.Create(deviceCtx, &Device{DeviceID: "dev-1", Name: "sensor"}))
	require.NoError(t, deviceRepo.Upsert(deviceCtx, &Device{DeviceID: "dev-1", Name: "sensor-v2"}))

	device, err := repository.By(deviceRepo, devices.DeviceID).Get(deviceCtx, "dev-1")
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

func TestBaseCompositePrimaryKeyByKeySet(t *testing.T) {
	repo, memberships, ctx := newMembershipRepo(t, "file:repository_composite_key_set_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &Membership{TenantID: 100, UserID: 200, Role: "viewer"}))

	key := repository.KeySet(
		repository.Part(memberships.TenantID, int64(100)),
		repository.Part(memberships.UserID, int64(200)),
	)

	legacyKey, err := key.Key()
	require.NoError(t, err)
	require.Equal(t, repository.Key{"tenant_id": int64(100), "user_id": int64(200)}, legacyKey)

	item, err := repo.GetByKeySet(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "viewer", item.Role)

	_, err = repo.UpdateByKeySet(ctx, key, memberships.Role.Set("admin"))
	require.NoError(t, err)

	updated, err := repo.GetByKeySet(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "admin", updated.Role)

	option, err := repo.GetByKeySetOption(ctx, repository.KeySet(
		repository.Part(memberships.TenantID, int64(100)),
		repository.Part(memberships.UserID, int64(404)),
	))
	require.NoError(t, err)
	require.False(t, option.IsPresent())

	_, err = repo.DeleteByKeySet(ctx, key)
	require.NoError(t, err)

	_, err = repo.GetByKeySet(ctx, key)
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestBaseKeySetValidation(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_key_set_validation_test?mode=memory&cache=shared", "alice")

	_, err := repo.GetByKeySet(ctx, repository.KeySet())
	require.ErrorIs(t, err, repository.ErrValidation)

	_, err = repository.KeySet(
		repository.Part(users.ID, int64(1)),
		repository.Part(users.ID, int64(2)),
	).Key()
	require.ErrorIs(t, err, repository.ErrValidation)
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

	noneByID, err := repository.By(repo, users.ID).GetOption(ctx, int64(99999))
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

func TestBaseTypedQueryResultAPIs(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_typed_query_test?mode=memory&cache=shared", "alice", "bob")

	query := querydsl.SelectFromInto[typedUserNameRow](users, users.ID, users.Name).
		Where(users.Name.Eq("alice"))
	items, err := repository.ListResult[typedUserNameRow](ctx, repo, query)
	require.NoError(t, err)
	require.Equal(t, 1, items.Len())

	first, err := repository.GetResult[typedUserNameRow](ctx, repo, query)
	require.NoError(t, err)
	require.Equal(t, "alice", first.Name)

	found, err := repository.FindResult[typedUserNameRow](ctx, repo, query)
	require.NoError(t, err)
	require.True(t, found.IsPresent())

	withMapper, err := repository.ListResultWithMapper[typedUserNameRow](
		ctx,
		repo,
		query,
		mapper.MustStructMapper[typedUserNameRow](),
	)
	require.NoError(t, err)
	require.Equal(t, 1, withMapper.Len())

	id, err := repository.ScalarResult[int64](ctx, repo, querydsl.SelectValue(users.ID).From(users).Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.EqualValues(t, 1, id)

	none, err := repository.ScalarResultOption[int64](ctx, repo, querydsl.SelectValue(users.ID).From(users).Where(users.Name.Eq("missing")))
	require.NoError(t, err)
	require.False(t, none.IsPresent())
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

func TestBaseUpdateByVersionSet(t *testing.T) {
	repo, users, ctx := newVersionedUserRepo(t, "file:repository_typed_version_conflict_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &VersionedUser{Name: "alice", Version: 1}))

	item, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users))
	require.NoError(t, err)

	key := repository.KeySet(repository.Part(users.ID, item.ID))
	_, err = repo.UpdateByVersionSet(ctx, key, users.Version, 1, users.Name.Set("alice-v2"))
	require.NoError(t, err)

	updated, err := repository.By(repo, users.ID).Get(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, "alice-v2", updated.Name)
	require.EqualValues(t, 2, updated.Version)

	_, err = repo.UpdateByVersionSet(ctx, key, users.Version, 1, users.Name.Set("alice-stale"))
	require.ErrorIs(t, err, repository.ErrVersionConflict)
}
