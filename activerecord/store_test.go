package activerecord_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/arcgolabs/dbx"
	activerecord "github.com/arcgolabs/dbx/activerecord"
	columnx "github.com/arcgolabs/dbx/column"
	sqlitedialect "github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/repository"
	schemax "github.com/arcgolabs/dbx/schema"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[User, string]                     `dbx:"name"`
}

func TestModelSaveReloadDelete(t *testing.T) {
	ctx, store := openUserStore(t, "file:activerecord_model_test?mode=memory&cache=shared")

	model := store.Wrap(&User{Name: "alice"})
	require.NoError(t, model.Save(ctx))
	require.NotZero(t, model.Entity().ID)

	model.Entity().Name = "alice-v2"
	require.NoError(t, model.Save(ctx))

	found, err := store.FindByID(ctx, model.Entity().ID)
	require.NoError(t, err)
	require.Equal(t, "alice-v2", found.Entity().Name)

	model.Entity().Name = "stale"
	require.NoError(t, model.Reload(ctx))
	require.Equal(t, "alice-v2", model.Entity().Name)

	require.NoError(t, model.Delete(ctx))

	_, err = store.FindByID(ctx, model.Entity().ID)
	require.True(t, errors.Is(err, repository.ErrNotFound))
}

func TestStoreFindOptionAPIs(t *testing.T) {
	ctx, store := openUserStore(t, "file:activerecord_option_test?mode=memory&cache=shared")

	model := store.Wrap(&User{Name: "alice"})
	require.NoError(t, model.Save(ctx))

	noneByID, err := store.FindByIDOption(ctx, int64(99999))
	require.NoError(t, err)
	require.False(t, noneByID.IsPresent())

	byID, err := store.FindByIDOption(ctx, model.Entity().ID)
	require.NoError(t, err)

	found, ok := byID.Get()
	require.True(t, ok)
	require.Equal(t, "alice", found.Entity().Name)

	byKey, err := store.FindByKeyOption(ctx, found.Key())
	require.NoError(t, err)

	again, ok := byKey.Get()
	require.True(t, ok)
	require.Equal(t, model.Entity().ID, again.Entity().ID)
}

func TestStoreTypedKeyAPIs(t *testing.T) {
	ctx, store := openUserStore(t, "file:activerecord_typed_key_test?mode=memory&cache=shared")
	users := store.Repository().Schema()

	model := store.Wrap(&User{Name: "alice"})
	require.NoError(t, model.Save(ctx))

	byID := activerecord.By(store, users.ID)
	exists, err := byID.Exists(ctx, model.Entity().ID)
	require.NoError(t, err)
	require.True(t, exists)

	found, err := byID.Find(ctx, model.Entity().ID)
	require.NoError(t, err)
	require.Equal(t, "alice", found.Entity().Name)

	none, err := byID.FindOption(ctx, int64(404))
	require.NoError(t, err)
	require.False(t, none.IsPresent())

	byName := activerecord.By(store, users.Name)
	foundByName, err := byName.Find(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, model.Entity().ID, foundByName.Entity().ID)
}

func TestStoreListPageBy(t *testing.T) {
	ctx, store := openUserStore(t, "file:activerecord_page_test?mode=memory&cache=shared")
	users := store.Repository().Schema()

	for _, name := range []string{"alice", "bob"} {
		model := store.Wrap(&User{Name: name})
		require.NoError(t, model.Save(ctx))
	}

	page, err := store.ListPageBy(ctx, 2, 1, repository.OrderBy(users.Name.Asc()))
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Equal(t, 2, page.Page)
	require.Equal(t, 1, page.PageSize)
	require.Equal(t, 1, page.Offset)
	require.False(t, page.HasNext)
	require.True(t, page.HasPrevious)
	require.Equal(t, 1, page.Items.Len())

	model, ok := page.Items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "bob", model.Entity().Name)
}

func openUserStore(tb testing.TB, dsn string) (context.Context, *activerecord.Store[User, UserSchema]) {
	tb.Helper()

	ctx := context.Background()
	raw, err := sql.Open("sqlite", dsn)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		if closeErr := raw.Close(); closeErr != nil {
			tb.Errorf("close sqlite: %v", closeErr)
		}
	})

	core := dbx.MustNewWithOptions(raw, sqlitedialect.New())
	users := schemax.MustSchema("users", UserSchema{})

	_, err = schemamigrate.AutoMigrate(ctx, core, users)
	require.NoError(tb, err)

	return ctx, activerecord.New[User, UserSchema](core, users)
}
