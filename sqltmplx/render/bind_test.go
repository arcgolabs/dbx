package render_test

import (
	"testing"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect/postgres"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/render"
	"github.com/stretchr/testify/require"
)

type bindQuery struct {
	Name string `db:"name"`
	IDs  []int  `json:"ids"`
}

func TestBindCommentPlaceholderWithStructTags(t *testing.T) {
	result, err := render.Render([]parse.Node{
		parse.TextNode{Text: "name = /* name */'bob' AND id IN (/* ids */(1, 2))"},
	}, bindQuery{Name: "alice", IDs: []int{10, 20}}, postgres.New())
	require.NoError(t, err)
	require.Equal(t, "name = $1 AND id IN ($2, $3)", result.Query)
	require.Equal(t, []any{"alice", 10, 20}, result.Args.Values())
}

func TestRenderListUsesCollectionNodes(t *testing.T) {
	result, err := render.RenderList(collectionx.NewList[parse.Node](
		parse.TextNode{Text: "name = /* name */'bob' AND id IN (/* ids */(1, 2))"},
	), bindQuery{Name: "alice", IDs: []int{10, 20}}, postgres.New())
	require.NoError(t, err)
	require.Equal(t, "name = $1 AND id IN ($2, $3)", result.Query)
	require.Equal(t, []any{"alice", 10, 20}, result.Args.Values())
}
