package sqltmpl_test

import (
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/dialect/sqlite"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
	"github.com/stretchr/testify/require"
)

func TestTemplateBindReturnsDBXBoundQuery(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("user/find_active.sql", `
select id, username
from users
where status = /* status */1
`)
	require.NoError(t, err)

	bound, err := template.Bind(struct {
		Status int
	}{Status: 1})
	require.NoError(t, err)
	require.Equal(t, "user/find_active.sql", bound.Name)
	require.Equal(t, 1, bound.Args.Len())
	value, ok := bound.Args.Get(0)
	require.True(t, ok)
	require.Equal(t, 1, value)
	require.NotEmpty(t, bound.SQL)
}

func TestEngineCompileUsesTemplateCache(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	text := `select id from users where status = /* status */1`

	first, err := engine.CompileNamed("users/find.sql", text)
	require.NoError(t, err)
	second, err := engine.CompileNamed("users/find.sql", text)
	require.NoError(t, err)

	require.Same(t, first, second)

	other, err := engine.CompileNamed("users/other.sql", text)
	require.NoError(t, err)
	require.NotSame(t, first, other)
}

func TestEngineCompileCacheCanBeDisabled(t *testing.T) {
	engine := sqltmpl.New(sqlite.New(), sqltmpl.WithTemplateCacheSize(0))
	text := `select id from users where status = /* status */1`

	first, err := engine.Compile(text)
	require.NoError(t, err)
	second, err := engine.Compile(text)
	require.NoError(t, err)

	require.NotSame(t, first, second)
}

func TestEngineAnalyzeAndCheck(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	text := `select id from users where status = /* status */1`

	metadata, err := engine.AnalyzeNamed("users/find.sql", text)
	require.NoError(t, err)
	require.Equal(t, "SELECT", metadata.StatementType)
	require.Equal(t, []string{"status"}, metadata.Parameters.Values())

	report, err := engine.CheckNamed("users/find.sql", text, struct {
		Status int
	}{Status: 1})
	require.NoError(t, err)
	require.Equal(t, "users/find.sql", report.Name)
	require.Equal(t, "sqlite", report.Dialect)
	require.Equal(t, sqltmpl.CheckStageOK, report.Stage)
	require.Equal(t, []any{1}, report.Args.Values())
	require.Equal(t, "SELECT", report.Analysis.StatementType)
}

func TestEngineCheckReportsCompileStage(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())

	report, err := engine.CheckNamed("users/bad.sql", `select /* %if missing(`, nil)
	require.Error(t, err)
	require.Equal(t, "users/bad.sql", report.Name)
	require.Equal(t, "sqlite", report.Dialect)
	require.Equal(t, sqltmpl.CheckStageCompile, report.Stage)
	require.False(t, report.SampleProvided)
}

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

func TestTemplateMetadata(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("sql/user/search.sql", `
SELECT id, username
FROM users
/*%where */
/*%if present(Tenant) */
  AND tenant = /* Tenant */'acme'
/*%end */
/*%if present(Status) */
  AND status = /* Status */'active'
/*%end */
/*%if !empty(IDs) */
  AND id IN (/* IDs */(1, 2, 3))
/*%end */
/*%end */
ORDER BY id
`)
	require.NoError(t, err)

	metadata := template.Metadata()
	require.Equal(t, "SELECT", metadata.StatementType)
	require.Equal(t, []string{"Tenant", "Status", "IDs"}, metadata.Parameters.Values())
	require.Equal(t, []string{"IDs"}, metadata.SpreadParameters.Values())
	require.Equal(t, []string{"present(Tenant)", "present(Status)", "!empty(IDs)"}, metadata.Conditions.Values())
	require.True(t, metadata.HasWhereBlock)
	require.False(t, metadata.HasSetBlock)
}

func TestTemplateCheck(t *testing.T) {
	engine := sqltmpl.New(sqlite.New())
	template, err := engine.CompileNamed("sql/user/find_active.sql", `
select id, username
from users
where status = /* status */1
`)
	require.NoError(t, err)

	report, err := template.Check(struct {
		Status int
	}{Status: 1})
	require.NoError(t, err)
	require.Equal(t, "sql/user/find_active.sql", report.Name)
	require.Equal(t, "sqlite", report.Dialect)
	require.Equal(t, sqltmpl.CheckStageOK, report.Stage)
	require.True(t, report.SampleProvided)
	require.Equal(t, "SELECT", report.Metadata.StatementType)
	require.Equal(t, []any{1}, report.Args.Values())
	require.NotEmpty(t, report.SQL)
	require.NotNil(t, report.Analysis)
	require.Equal(t, "SELECT", report.Analysis.StatementType)
	require.NoError(t, report.Err)
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
