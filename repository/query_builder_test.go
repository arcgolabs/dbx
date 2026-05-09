package repository_test

import (
	"context"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/paging"
	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

func TestQueryBuilderListsCountsAndFinds(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_query_builder_test?mode=memory&cache=shared", "alice", "bob", "alex")

	query := repository.Query(repo).
		Where(users.Name.Ge("alex")).
		OrderBy(users.Name.Asc())

	items, err := query.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, items.Len())
	first, ok := items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alex", first.Name)

	count, err := query.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	exists, err := query.Where(users.Name.Eq("alice")).Exists(ctx)
	require.NoError(t, err)
	require.True(t, exists)

	found, err := query.Where(users.Name.Eq("alice")).Find(ctx)
	require.NoError(t, err)
	require.True(t, found.IsPresent())
	foundUser, ok := found.Get()
	require.True(t, ok)
	require.Equal(t, "alice", foundUser.Name)

	missing, err := query.Where(users.Name.Eq("nobody")).FirstOption(ctx)
	require.NoError(t, err)
	require.False(t, missing.IsPresent())
}

func TestQueryBuilderListPage(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_query_builder_page_test?mode=memory&cache=shared", "alice", "bob", "carol")

	result, err := repository.Query(repo).
		OrderBy(users.ID.Asc()).
		ListPage(ctx, paging.NewRequest(2, 2))
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Total)
	require.Equal(t, 1, result.Items.Len())
	item, ok := result.Items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "carol", item.Name)
}

func TestQueryBuilderIncludeLoadsResults(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_query_builder_include_test?mode=memory&cache=shared", "alice", "bob")
	loaded := 0
	include := repository.IncludeFunc[User](func(_ context.Context, items *collectionx.List[User]) error {
		loaded += items.Len()
		items.SetAllIndexed(func(_ int, item User) User {
			item.Name += "-loaded"
			return item
		})
		return nil
	})

	items, err := repository.Query(repo).
		OrderBy(users.ID.Asc()).
		Include(include).
		List(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, loaded)
	first, ok := items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alice-loaded", first.Name)

	item, err := repository.Query(repo).
		Where(users.Name.Eq("bob")).
		Include(include).
		First(ctx)
	require.NoError(t, err)
	require.Equal(t, "bob-loaded", item.Name)

	page, err := repository.Query(repo).
		OrderBy(users.ID.Asc()).
		Include(include).
		ListPage(ctx, paging.NewRequest(1, 1))
	require.NoError(t, err)
	pageItem, ok := page.Items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "alice-loaded", pageItem.Name)
	require.Equal(t, 4, loaded)
}

func TestQueryBuilderNilRepository(t *testing.T) {
	ctx := context.Background()
	query := repository.Query[User, UserSchema](nil)

	_, err := query.List(ctx)
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = query.First(ctx)
	require.ErrorIs(t, err, dbx.ErrNilDB)
	option, err := query.Find(ctx)
	require.ErrorIs(t, err, dbx.ErrNilDB)
	require.False(t, option.IsPresent())
	_, err = query.Count(ctx)
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = query.Exists(ctx)
	require.ErrorIs(t, err, dbx.ErrNilDB)
	_, err = query.ListPage(ctx, paging.NewRequest(1, 20))
	require.ErrorIs(t, err, dbx.ErrNilDB)
}
