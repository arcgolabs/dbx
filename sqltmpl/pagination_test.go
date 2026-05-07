package sqltmpl_test

import (
	"testing"

	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
	"github.com/stretchr/testify/require"
)

func TestTemplateRenderPageInjectsNormalizedPagination(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
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
	}{Status: 1}, sqltmpl.Page(2, 10))
	require.NoError(t, err)
	require.Equal(t, "select id, username from users where status = ? and id > 0 limit ? offset ?", bound.Query)
	require.Equal(t, []any{1, 10, 10}, bound.Args.Values())
}

func TestTemplateBindPageSetsCapacityHint(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("users/page.sql", `
select id, username
from users
limit /* page.limit */20 offset /* page.offset */0
`)
	require.NoError(t, err)

	bound, err := template.BindPage(nil, sqltmpl.Page(3, 5))
	require.NoError(t, err)
	require.Equal(t, "users/page.sql", bound.Name)
	require.Equal(t, "select id, username from users limit ? offset ?", bound.SQL)
	require.Equal(t, []any{5, 10}, bound.Args.Values())
	require.Equal(t, 5, bound.CapacityHint)
}

func TestTemplateRenderWithTypedPage(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.Compile(`
select id, username
from users
where status = /* status */1
limit /* Page.Limit */20 offset /* Page.Offset */0
`)
	require.NoError(t, err)

	type params struct {
		Status int `db:"status"`
	}

	paged := sqltmpl.WithTypedPage(params{Status: 1}, sqltmpl.Page(2, 10))
	bound, err := template.Render(paged)
	require.NoError(t, err)
	require.Equal(t, params{Status: 1}, paged.Params)
	require.Equal(t, 10, paged.Page.Limit())
	require.Equal(t, "select id, username from users where status = ? limit ? offset ?", bound.Query)
	require.Equal(t, []any{1, 10, 10}, bound.Args.Values())
}

func TestEngineRenderPage(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())

	bound, err := engine.RenderPage(
		"select id from users limit /* Page.Limit */20 offset /* Page.Offset */0",
		map[string]any{},
		sqltmpl.NewPageRequest(1, 0),
	)
	require.NoError(t, err)
	require.Equal(t, "select id from users limit ? offset ?", bound.Query)
	require.Equal(t, []any{20, 0}, bound.Args.Values())
}
