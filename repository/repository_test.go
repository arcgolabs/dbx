package repository_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	sqlitedialect "github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func allColumns(schema schemax.Resource) collectionx.List[querydsl.SelectItem] {
	return collectionx.MapList[schemax.ColumnMeta, querydsl.SelectItem](schema.Spec().Columns, func(_ int, column schemax.ColumnMeta) querydsl.SelectItem {
		return columnx.Named[any](schema, column.Name)
	})
}

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.Column[User, int64]  `dbx:"id,pk,auto"`
	Name columnx.Column[User, string] `dbx:"name"`
}

type Device struct {
	DeviceID string `dbx:"device_id"`
	Name     string `dbx:"name"`
}

type DeviceSchema struct {
	schemax.Schema[Device]
	DeviceID columnx.Column[Device, string] `dbx:"device_id,pk"`
	Name     columnx.Column[Device, string] `dbx:"name"`
}

type Membership struct {
	TenantID int64  `dbx:"tenant_id"`
	UserID   int64  `dbx:"user_id"`
	Role     string `dbx:"role"`
}

type MembershipSchema struct {
	schemax.Schema[Membership]
	TenantID columnx.Column[Membership, int64]  `dbx:"tenant_id"`
	UserID   columnx.Column[Membership, int64]  `dbx:"user_id"`
	Role     columnx.Column[Membership, string] `dbx:"role"`
	PK       schemax.CompositeKey[Membership]   `key:"columns=tenant_id|user_id"`
}

type VersionedUser struct {
	ID      int64  `dbx:"id"`
	Name    string `dbx:"name"`
	Version int64  `dbx:"version"`
}

type VersionedUserSchema struct {
	schemax.Schema[VersionedUser]
	ID      columnx.Column[VersionedUser, int64]  `dbx:"id,pk,auto"`
	Name    columnx.Column[VersionedUser, string] `dbx:"name"`
	Version columnx.Column[VersionedUser, int64]  `dbx:"version,default=1"`
}

func TestNewUsesSchemaAsMetadataSource(t *testing.T) {
	core := dbx.New((*sql.DB)(nil), sqlitedialect.New())
	users := schemax.MustSchema("users", UserSchema{})
	repo := repository.New[User](core, users)

	require.Same(t, core, repo.DB())
	require.Equal(t, "users", repo.Schema().TableName())

	_, ok := repo.Mapper().FieldByColumn("name")
	require.True(t, ok)
}

func TestBaseCreateListAndFirst(t *testing.T) {
	repo, users, ctx := newUserRepo(t, "file:repository_crud_test?mode=memory&cache=shared")
	seedUsers(ctx, t, repo, "alice")

	items, err := repo.List(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, items.Len())
	item, ok := items.Get(0)
	require.True(t, ok)
	require.Equal(t, "alice", item.Name)

	item, err = repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.Equal(t, "alice", item.Name)
}

func TestBaseFirstNotFound(t *testing.T) {
	repo, users, ctx := newUserRepo(t, "file:repository_not_found_test?mode=memory&cache=shared")

	_, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("nobody")))
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestBaseGetByIDCountExistsUpdateDeleteByIDAndListPage(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_features_test?mode=memory&cache=shared", "alice", "bob")

	total, err := repo.Count(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)

	exists, err := repo.Exists(ctx, querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("alice")))
	require.NoError(t, err)
	require.True(t, exists)

	alice, err := repo.First(ctx, querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("alice")))
	require.NoError(t, err)

	got, err := repo.GetByID(ctx, alice.ID)
	require.NoError(t, err)
	require.Equal(t, "alice", got.Name)

	_, err = repo.UpdateByID(ctx, alice.ID, users.Name.Set("alice-updated"))
	require.NoError(t, err)

	updated, err := repo.GetByID(ctx, alice.ID)
	require.NoError(t, err)
	require.Equal(t, "alice-updated", updated.Name)

	page, err := repo.ListPage(ctx, querydsl.Select(allColumns(users).Values()...).From(users).OrderBy(users.Name.Asc()), 1, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Equal(t, 1, page.Page)
	require.Equal(t, 1, page.PageSize)
	require.Equal(t, 0, page.Offset)
	require.Equal(t, 2, page.TotalPages)
	require.True(t, page.HasNext)
	require.False(t, page.HasPrevious)
	require.Equal(t, 1, page.Items.Len())

	_, err = repo.DeleteByID(ctx, alice.ID)
	require.NoError(t, err)

	afterDelete, err := repo.Count(ctx, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, afterDelete)
}

func TestBaseFirstDoesNotMutateQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_first_immutable_test?mode=memory&cache=shared", "alice")

	query := querydsl.Select(allColumns(users).Values()...).From(users).Where(users.Name.Eq("alice"))
	_, err := repo.First(ctx, query)
	require.NoError(t, err)
	require.Nil(t, query.LimitN)
	require.Nil(t, query.OffsetN)
}

func TestBaseListDoesNotMutateQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_list_immutable_test?mode=memory&cache=shared", "alice", "bob")

	query := newOrderedUserQuery(users)
	_, err := repo.List(ctx, query)
	require.NoError(t, err)
	assertOrderedUserQueryUnchanged(t, query)
}

func TestBaseCountDoesNotMutateQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_count_immutable_test?mode=memory&cache=shared", "alice", "bob")

	query := newOrderedUserQuery(users)
	_, err := repo.Count(ctx, query)
	require.NoError(t, err)
	assertOrderedUserQueryUnchanged(t, query)
}

func TestBaseListPageDoesNotMutateQuery(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_page_immutable_test?mode=memory&cache=shared", "alice", "bob")

	query := querydsl.Select(allColumns(users).Values()...).From(users).OrderBy(users.Name.Asc())
	_, err := repo.ListPage(ctx, query, 2, 1)
	require.NoError(t, err)
	require.Nil(t, query.LimitN)
	require.Nil(t, query.OffsetN)
}

func newUserRepo(t *testing.T, dsn string) (*repository.Base[User, UserSchema], UserSchema, context.Context) {
	t.Helper()

	ctx := context.Background()
	core := openRepositoryCore(t, dsn)
	users := schemax.MustSchema("users", UserSchema{})
	mustAutoMigrate(ctx, t, core, users)

	return repository.New[User](core, users), users, ctx
}

func newDeviceRepo(t *testing.T, dsn string) (*repository.Base[Device, DeviceSchema], DeviceSchema, context.Context) {
	t.Helper()

	ctx := context.Background()
	core := openRepositoryCore(t, dsn)
	devices := schemax.MustSchema("devices", DeviceSchema{})
	mustAutoMigrate(ctx, t, core, devices)

	return repository.New[Device](core, devices), devices, ctx
}

func newMembershipRepo(t *testing.T, dsn string) (*repository.Base[Membership, MembershipSchema], MembershipSchema, context.Context) {
	t.Helper()

	ctx := context.Background()
	core := openRepositoryCore(t, dsn)
	memberships := schemax.MustSchema("memberships", MembershipSchema{})
	mustAutoMigrate(ctx, t, core, memberships)

	return repository.New[Membership](core, memberships), memberships, ctx
}

func newVersionedUserRepo(t *testing.T, dsn string) (*repository.Base[VersionedUser, VersionedUserSchema], VersionedUserSchema, context.Context) {
	t.Helper()

	ctx := context.Background()
	core := openRepositoryCore(t, dsn)
	users := schemax.MustSchema("versioned_users", VersionedUserSchema{})
	mustAutoMigrate(ctx, t, core, users)

	return repository.New[VersionedUser](core, users), users, ctx
}

func newSeededUserRepo(t *testing.T, dsn string, names ...string) (*repository.Base[User, UserSchema], UserSchema, context.Context) {
	t.Helper()

	repo, users, ctx := newUserRepo(t, dsn)
	seedUsers(ctx, t, repo, names...)

	return repo, users, ctx
}

func openRepositoryCore(t *testing.T, dsn string) *dbx.DB {
	t.Helper()

	raw, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Errorf("close sqlite: %v", closeErr)
		}
	})

	return dbx.MustNewWithOptions(raw, sqlitedialect.New())
}

func mustAutoMigrate(ctx context.Context, t *testing.T, core *dbx.DB, schemas ...schemamigrate.Resource) {
	t.Helper()

	_, err := schemamigrate.AutoMigrate(ctx, core, schemas...)
	require.NoError(t, err)
}

func seedUsers(ctx context.Context, t *testing.T, repo *repository.Base[User, UserSchema], names ...string) {
	t.Helper()

	for _, name := range names {
		require.NoError(t, repo.Create(ctx, &User{Name: name}))
	}
}

func newOrderedUserQuery(users UserSchema) *querydsl.SelectQuery {
	return querydsl.Select(allColumns(users).Values()...).From(users).OrderBy(users.Name.Asc()).Limit(10).Offset(5)
}

func assertOrderedUserQueryUnchanged(t *testing.T, query *querydsl.SelectQuery) {
	t.Helper()

	require.NotNil(t, query.LimitN)
	require.Equal(t, 10, *query.LimitN)
	require.NotNil(t, query.OffsetN)
	require.Equal(t, 5, *query.OffsetN)
	require.Equal(t, 1, query.Orders.Len())
}
