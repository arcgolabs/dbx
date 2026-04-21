package render_test

import (
	"testing"

	"github.com/arcgolabs/dbx/dialect/postgres"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/render"
	"github.com/stretchr/testify/require"
)

type nestedFilter struct {
	IDs []int `json:"ids"`
}

type queryParams struct {
	Name   string       `db:"name"`
	Status string       `json:"status"`
	Filter nestedFilter `json:"filter"`
}

func (p queryParams) UpperStatus() string {
	return p.Status
}

func TestLookupPrefersFieldThenTag(t *testing.T) {
	params := queryParams{
		Name:   "alice",
		Status: "ACTIVE",
		Filter: nestedFilter{IDs: []int{1, 2, 3}},
	}

	result, err := render.Render([]parse.Node{
		parse.TextNode{Text: "name = /* Name */'x' AND alias = /* name */'x' AND status = /* status */'x' AND id IN (/* filter.ids */(1, 2, 3))"},
	}, params, postgres.New())
	require.NoError(t, err)
	require.Equal(t, "name = $1 AND alias = $2 AND status = $3 AND id IN ($4, $5, $6)", result.Query)
	require.Equal(t, []any{"alice", "alice", "ACTIVE", 1, 2, 3}, result.Args.Values())
}

func TestLookupSupportsZeroArgMethods(t *testing.T) {
	params := queryParams{Status: "ACTIVE"}

	result, err := render.Render([]parse.Node{
		parse.TextNode{Text: "status = /* upperstatus */'x'"},
	}, params, postgres.New())
	require.NoError(t, err)
	require.Equal(t, "status = $1", result.Query)
	require.Equal(t, []any{"ACTIVE"}, result.Args.Values())
}
