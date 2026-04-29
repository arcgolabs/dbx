package paging_test

import (
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/paging"
	"github.com/stretchr/testify/require"
)

func TestRequestNormalize(t *testing.T) {
	request := paging.Page(0, 0).WithMaxPageSize(10)

	require.Equal(t, 1, request.Page)
	require.Equal(t, 10, request.PageSize)
	require.Equal(t, 10, request.Limit())
	require.Equal(t, 0, request.Offset())
}

func TestNewResultBuildsMetadata(t *testing.T) {
	result := paging.NewResult[string](collectionx.NewList[string]("alice"), 21, paging.Page(2, 10))

	require.EqualValues(t, 21, result.Total)
	require.Equal(t, 1, result.Items.Len())
	require.Equal(t, 2, result.Page)
	require.Equal(t, 10, result.PageSize)
	require.Equal(t, 10, result.Offset)
	require.Equal(t, 3, result.TotalPages)
	require.True(t, result.HasNext)
	require.True(t, result.HasPrevious)
}
