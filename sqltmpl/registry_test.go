package sqltmpl_test

import (
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/dialect/postgres"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
	"github.com/stretchr/testify/require"
)

func TestRegistryLoadsAndCachesTemplates(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_active.sql": {
			Data: []byte(`
select id, username
from users
where status = /* status */1
order by id
`),
		},
	}, sqlite.New())

	first, err := registry.Template("sql/user/find_active.sql")
	require.NoError(t, err)

	second, err := registry.Statement("/sql/user/find_active.sql")
	require.NoError(t, err)
	require.Same(t, first, second)

	bound, err := second.Bind(struct {
		Status int
	}{Status: 2})
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_active.sql", bound.Name)
	require.Equal(t, []any{2}, bound.Args.Values())
}

func TestRegistryPrefersDialectTemplate(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find.sql": {
			Data: []byte(`select 'base' as flavor`),
		},
		"sql/user/find_sqlite.sql": {
			Data: []byte(`select 'sqlite' as flavor`),
		},
		"sql/user/find__sqlite.sql": {
			Data: []byte(`select 'sqlite legacy' as flavor`),
		},
	}, sqlite.New())

	template, err := registry.Template("sql/user/find.sql")
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_sqlite.sql", template.StatementName())

	bound, err := template.Bind(nil)
	require.NoError(t, err)
	require.Contains(t, bound.SQL, "'sqlite'")

	again, err := registry.Template("/sql/user/find.sql")
	require.NoError(t, err)
	require.Same(t, template, again)
}

func TestRegistryResolvesDialectTemplateWithoutBaseTemplate(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_sqlite.sql": {
			Data: []byte(`select 'sqlite' as flavor`),
		},
	}, sqlite.New())

	template, err := registry.Statement("sql/user/find.sql")
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_sqlite.sql", template.StatementName())
}

func TestRegistryFallsBackToDoubleUnderscoreDialectTemplate(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find__sqlite.sql": {
			Data: []byte(`select 'sqlite' as flavor`),
		},
	}, sqlite.New())

	template, err := registry.Statement("sql/user/find.sql")
	require.NoError(t, err)
	require.Equal(t, "sql/user/find__sqlite.sql", template.StatementName())
}

func TestRegistryLoadsExplicitDialectTemplateName(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_mysql.sql": {
			Data: []byte(`select 'mysql' as flavor`),
		},
		"sql/user/find__mysql.sql": {
			Data: []byte(`select 'mysql legacy' as flavor`),
		},
		"sql/user/find__sqlite.sql": {
			Data: []byte(`select 'sqlite' as flavor`),
		},
	}, sqlite.New())

	template, err := registry.Template("sql/user/find_mysql.sql")
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_mysql.sql", template.StatementName())

	bound, err := template.Bind(nil)
	require.NoError(t, err)
	require.Contains(t, bound.SQL, "'mysql'")
}

func TestRegistryStatementForUsesCallTimeDialect(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find.sql": {
			Data: []byte(`select id from users where id = /* ID */1`),
		},
		"sql/user/find_postgres.sql": {
			Data: []byte(`select id from users where id = /* ID */1`),
		},
	}, sqlite.New())

	template, err := registry.StatementFor("sql/user/find.sql", postgres.New())
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_postgres.sql", template.StatementName())

	bound, err := template.Bind(struct {
		ID int64
	}{ID: 42})
	require.NoError(t, err)
	require.Contains(t, bound.SQL, "$1")
	require.Equal(t, []any{int64(42)}, bound.Args.Values())

	defaultTemplate, err := registry.Statement("sql/user/find.sql")
	require.NoError(t, err)
	require.Equal(t, "sql/user/find.sql", defaultTemplate.StatementName())

	defaultBound, err := defaultTemplate.Bind(struct {
		ID int64
	}{ID: 42})
	require.NoError(t, err)
	require.Contains(t, defaultBound.SQL, "?")
}

func TestRegistryTypedForUsesCallTimeDialect(t *testing.T) {
	type findParams struct {
		ID int64
	}

	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find.sql": {
			Data: []byte(`select id from users where id = /* ID */1`),
		},
		"sql/user/find_postgres.sql": {
			Data: []byte(`select id from users where id = /* ID */1`),
		},
	}, sqlite.New())

	source, err := sqltmpl.TypedFor[findParams](registry, "sql/user/find.sql", postgres.New())
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_postgres.sql", source.StatementName())

	bound, err := source.Bind(findParams{ID: 42})
	require.NoError(t, err)
	require.Contains(t, bound.SQL, "$1")
	require.Equal(t, []any{int64(42)}, bound.Args.Values())

	statement := sqltmpl.MustLoadScalarStatementFor[findParams, int64](registry, "sql/user/find.sql", postgres.New())
	require.Equal(t, "sql/user/find_postgres.sql", statement.StatementName())
}

func TestRegistryPreloadAndPreloadAll(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_active.sql": {
			Data: []byte(`select id from users where status = /* status */1`),
		},
		"sql/user/find_many.sql": {
			Data: []byte(`select id from users where id in (/* ids */(1, 2))`),
		},
	}, sqlite.New())

	preloaded, err := registry.Preload("sql/user/find_active.sql")
	require.NoError(t, err)
	require.Len(t, preloaded.Values(), 1)

	all, err := registry.PreloadAll()
	require.NoError(t, err)
	require.Len(t, all.Values(), 2)

	first, ok := all.Get(0)
	require.True(t, ok)
	require.Equal(t, "sql/user/find_active.sql", first.StatementName())
}

func TestRegistryNamesAndCheckAll(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{
		"sql/user/find_active.sql": {
			Data: []byte(`
select id, username
from users
where status = /* status */1
`),
		},
		"sql/user/find_many.sql": {
			Data: []byte(`
select id, username
from users
where id in (/* ids */(1, 2))
`),
		},
		"sql/user/readme.txt": {
			Data: []byte("ignored"),
		},
	}, sqlite.New())

	names, err := registry.Names()
	require.NoError(t, err)
	require.Equal(t, []string{"sql/user/find_active.sql", "sql/user/find_many.sql"}, names.Values())

	reports, err := registry.CheckAll(map[string]any{
		"sql/user/find_active.sql": struct {
			Status int
		}{Status: 2},
	})
	require.NoError(t, err)
	require.Len(t, reports.Values(), 2)

	first, ok := reports.Get(0)
	require.True(t, ok)
	require.Equal(t, "sql/user/find_active.sql", first.Name)
	require.NoError(t, first.Err)
	require.Equal(t, sqltmpl.CheckStageOK, first.Stage)
	require.True(t, first.SampleProvided)
	require.Equal(t, "SELECT", first.Analysis.StatementType)

	second, ok := reports.Get(1)
	require.True(t, ok)
	require.Equal(t, "sql/user/find_many.sql", second.Name)
	require.ErrorContains(t, second.Err, `parameter "ids" not found`)
	require.Equal(t, sqltmpl.CheckStageRender, second.Stage)
	require.False(t, second.SampleProvided)
	require.Equal(t, []string{"ids"}, second.Metadata.Parameters.Values())
	require.Equal(t, []string{"ids"}, second.Metadata.SpreadParameters.Values())
}

func TestRegistryCheckMissingTemplateReportsLoadStage(t *testing.T) {
	registry := sqltmpl.NewRegistry(fstest.MapFS{}, sqlite.New())

	report, err := registry.Check("sql/missing.sql", nil)
	require.Error(t, err)
	require.Equal(t, sqltmpl.CheckStageLoad, report.Stage)
	require.Equal(t, "sql/missing.sql", report.Name)
	require.False(t, report.SampleProvided)
}
