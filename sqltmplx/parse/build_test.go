package parse_test

import (
	"testing"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/scan"
	"github.com/stretchr/testify/require"
)

func TestBuildList(t *testing.T) {
	t.Parallel()

	nodes, err := parse.BuildList(collectionx.NewList[scan.Token](
		scan.Token{Kind: scan.Text, Value: "select * from users "},
		scan.Token{Kind: scan.Directive, Value: "%where"},
		scan.Token{Kind: scan.Text, Value: "name = /* name */'bob'"},
		scan.Token{Kind: scan.Directive, Value: "%end"},
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
	require.Equal(t, 1, where.Body.Len())
}
