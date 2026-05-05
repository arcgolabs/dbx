package repository_test

import (
	"testing"

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
