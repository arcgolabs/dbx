package sqltmpl_test

import (
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
	"github.com/stretchr/testify/require"
)

func TestNewScalarStatementBindsTypedParams(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("user/count_active.sql", `
select count(*)
from users
where status = /* Status */1
`)
	require.NoError(t, err)

	statement := sqltmpl.NewScalarStatement[typedFindUserParams, int64](template)
	bound, err := statement.Bind(typedFindUserParams{Status: 1})
	require.NoError(t, err)

	require.Equal(t, "user/count_active.sql", statement.StatementName())
	require.Equal(t, "user/count_active.sql", bound.Name)
	require.Equal(t, []any{1}, bound.Args.Values())
	require.NotEmpty(t, bound.SQL)
}

func TestLoadScalarStatementLoadsTemplateFromRegistry(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/count_active.sql": {
			Data: []byte(`
select count(*)
from users
where status = /* Status */1
`),
		},
	}, sqlite.New())

	statement, err := sqltmpl.LoadScalarStatement[typedFindUserParams, int64](registry, "sql/user/count_active.sql")
	require.NoError(t, err)

	bound, err := statement.Bind(typedFindUserParams{Status: 2})
	require.NoError(t, err)
	require.Equal(t, "sql/user/count_active.sql", bound.Name)
	require.Equal(t, []any{2}, bound.Args.Values())
}
