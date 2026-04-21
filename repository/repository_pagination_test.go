package repository_test

import (
	"testing"

	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/querydsl"
	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

func TestBaseListPageRequestAndPageSpecSharePagination(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_page_request_test?mode=memory&cache=shared", "alice", "bob")

	page, err := repo.ListPageSpecRequest(ctx, paging.Page(2, 1), repository.OrderBy(users.Name.Asc()))
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Equal(t, 2, page.Page)
	require.Equal(t, 1, page.PageSize)
	require.Equal(t, 1, page.Offset)
	require.Equal(t, 2, page.TotalPages)
	require.False(t, page.HasNext)
	require.True(t, page.HasPrevious)
	require.Equal(t, 1, page.Items.Len())
	second, ok := page.Items.GetFirst()
	require.True(t, ok)
	require.Equal(t, "bob", second.Name)

	items, err := repo.ListSpec(ctx, repository.OrderBy(users.Name.Asc()), repository.Page(2, 1))
	require.NoError(t, err)
	require.Equal(t, 1, items.Len())
	item, ok := items.GetFirst()
	require.True(t, ok)
	require.Equal(t, second.ID, item.ID)
}

func TestBaseCountWrapsComplexQueries(t *testing.T) {
	repo, users, ctx := newSeededUserRepo(t, "file:repository_count_wrap_test?mode=memory&cache=shared", "alice", "alice", "bob")

	distinctNames := querydsl.Select(users.Name).From(users).WithDistinct()
	total, err := repo.Count(ctx, distinctNames)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)

	groupedNames := querydsl.Select(users.Name).From(users).GroupBy(users.Name)
	total, err = repo.Count(ctx, groupedNames)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
}
