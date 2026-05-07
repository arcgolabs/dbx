package sqltmpl_test

import (
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
	"github.com/stretchr/testify/require"
)

type typedFindUserParams struct {
	Status int
}

type typedUserRow struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

func TestNewStructStatementBindsTypedParams(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("user/find_active.sql", `
select id, username
from users
where status = /* Status */1
`)
	require.NoError(t, err)

	statement := sqltmpl.NewStructStatement[typedFindUserParams, typedUserRow](template)
	bound, err := statement.Bind(typedFindUserParams{Status: 1})
	require.NoError(t, err)

	require.Equal(t, "user/find_active.sql", statement.StatementName())
	require.Equal(t, "user/find_active.sql", bound.Name)
	require.Equal(t, []any{1}, bound.Args.Values())
	require.NotEmpty(t, bound.SQL)
}

func TestLoadStructStatementLoadsTemplateFromRegistry(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_active.sql": {
			Data: []byte(`
select id, username
from users
where status = /* Status */1
`),
		},
	}, sqlite.New())

	statement, err := sqltmpl.LoadStructStatement[typedFindUserParams, typedUserRow](registry, "sql/user/find_active.sql")
	require.NoError(t, err)

	bound, err := statement.Bind(typedFindUserParams{Status: 2})
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_active.sql", bound.Name)
	require.Equal(t, []any{2}, bound.Args.Values())
}
