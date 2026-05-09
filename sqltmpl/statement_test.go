package sqltmpl_test

import (
	"testing"

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
