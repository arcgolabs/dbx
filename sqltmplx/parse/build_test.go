package parse_test

import (
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/scan"
	"github.com/stretchr/testify/require"
)

func TestBuildList(t *testing.T) {
	t.Parallel()

	nodes, err := parse.BuildList(collectionx.NewList[scan.Token](
		scan.Token{Kind: scan.Text, Value: "select * from users ", Span: scan.Span{Start: scan.Position{Line: 1, Column: 1}, End: scan.Position{Offset: 20, Line: 1, Column: 21}}},
		scan.Token{Kind: scan.Directive, Value: "%where", Span: scan.Span{Start: scan.Position{Offset: 20, Line: 1, Column: 21}, End: scan.Position{Offset: 31, Line: 1, Column: 32}}},
		scan.Token{Kind: scan.Text, Value: "name = /* name */'bob'", Span: scan.Span{Start: scan.Position{Offset: 31, Line: 1, Column: 32}, End: scan.Position{Offset: 53, Line: 1, Column: 54}}},
		scan.Token{Kind: scan.Directive, Value: "%end", Span: scan.Span{Start: scan.Position{Offset: 53, Line: 1, Column: 54}, End: scan.Position{Offset: 62, Line: 1, Column: 63}}},
	))
	require.NoError(t, err)
	require.Equal(t, 2, nodes.Len())

	first, ok := nodes.Get(0)
	require.True(t, ok)
	require.IsType(t, parse.TextNode{}, first)

	second, ok := nodes.Get(1)
	require.True(t, ok)
	where, ok := second.(*parse.WhereNode)
	require.True(t, ok)
	require.Equal(t, 2, where.Body.Len())

	paramNode, ok := where.Body.Get(1)
	require.True(t, ok)
	param, ok := paramNode.(parse.ParamNode)
	require.True(t, ok)
	require.Equal(t, "name", param.Name)
	require.False(t, param.Spread)
	require.Equal(t, scan.Position{Offset: 38, Line: 1, Column: 39}, param.Span.Start)
}

func TestBuildListReturnsPlaceholderLocationError(t *testing.T) {
	t.Parallel()

	_, err := parse.BuildList(collectionx.NewList[scan.Token](
		scan.Token{
			Kind:  scan.Text,
			Value: "id in (/* ids */",
			Span:  scan.Span{Start: scan.Position{Line: 3, Column: 5}, End: scan.Position{Offset: 16, Line: 3, Column: 21}},
		},
	))
	require.EqualError(t, err, "sqltmplx: placeholder \"ids\" missing test literal at 3:12")
}
