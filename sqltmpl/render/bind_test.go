package render_test

import (
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect/postgres"
	"github.com/arcgolabs/dbx/sqltmpl/parse"
	"github.com/arcgolabs/dbx/sqltmpl/render"
	"github.com/arcgolabs/dbx/sqltmpl/scan"
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

func TestRenderCompiledTemplateUsesParamNode(t *testing.T) {
	tokens, err := scan.ScanList("name = /* name */'bob' AND id IN (/* ids */(1, 2))")
	require.NoError(t, err)
	nodes, err := parse.BuildList(tokens)
	require.NoError(t, err)

	result, err := render.RenderList(nodes, bindQuery{Name: "alice", IDs: []int{10, 20}}, postgres.New())
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
