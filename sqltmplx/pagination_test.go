package sqltmplx_test

import (
	"testing"

	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmplx "github.com/arcgolabs/dbx/sqltmplx"
	"github.com/stretchr/testify/require"
)

func TestTemplateRenderPageInjectsNormalizedPagination(t *testing.T) {
	engine := sqltmplx.New(sqlite.New())
	template, err := engine.Compile(`
select id, username
from users
where status = /* status */1
/*% if Page.Page > 1 */
and id > 0
/*% end */
limit /* Page.Limit */20 offset /* Page.Offset */0
`)
	require.NoError(t, err)

	bound, err := template.RenderPage(struct {
		Status int `db:"status"`
	}{Status: 1}, sqltmplx.Page(2, 10))
	require.NoError(t, err)
	require.Equal(t, "select id, username from users where status = ? and id > 0 limit ? offset ?", bound.Query)
	require.Equal(t, []any{1, 10, 10}, bound.Args.Values())
}

func TestTemplateBindPageSetsCapacityHint(t *testing.T) {
	engine := sqltmplx.New(sqlite.New())
	template, err := engine.CompileNamed("users/page.sql", `
select id, username
from users
limit /* page.limit */20 offset /* page.offset */0
`)
	require.NoError(t, err)

	bound, err := template.BindPage(nil, sqltmplx.Page(3, 5))
	require.NoError(t, err)
	require.Equal(t, "users/page.sql", bound.Name)
	require.Equal(t, "select id, username from users limit ? offset ?", bound.SQL)
	require.Equal(t, []any{5, 10}, bound.Args.Values())
	require.Equal(t, 5, bound.CapacityHint)
}

func TestEngineRenderPage(t *testing.T) {
	engine := sqltmplx.New(sqlite.New())

	bound, err := engine.RenderPage(
		"select id from users limit /* Page.Limit */20 offset /* Page.Offset */0",
		map[string]any{},
		sqltmplx.NewPageRequest(1, 0),
	)
	require.NoError(t, err)
	require.Equal(t, "select id from users limit ? offset ?", bound.Query)
	require.Equal(t, []any{20, 0}, bound.Args.Values())
}
