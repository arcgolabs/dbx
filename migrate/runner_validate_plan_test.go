package migrate_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
)

func TestRunnerPlanBaselineValidateAndRepairSQL(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-validate-plan.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql": &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
		},
		Dir: "sql",
	}

	plan, err := runner.DryRunSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 1, plan.Len())

	baselined, err := runner.BaselineSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 1, baselined.Len())
	require.False(t, sqliteTableExists(ctx, t, db, "logs"))

	plan, err = runner.PlanSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 0, plan.Len())

	report, err := runner.ValidateSQL(ctx, source)
	require.NoError(t, err)
	require.True(t, report.Valid())

	changedSource := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql": &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY, name TEXT);\n")},
		},
		Dir: "sql",
	}
	report, err = runner.ValidateSQL(ctx, changedSource)
	require.NoError(t, err)
	require.False(t, report.Valid())
	require.Equal(t, 1, report.Issues.Len())

	repaired, err := runner.RepairSQL(ctx, changedSource)
	require.NoError(t, err)
	require.Equal(t, 1, repaired.Len())

	report, err = runner.ValidateSQL(ctx, changedSource)
	require.NoError(t, err)
	require.True(t, report.Valid())
}
