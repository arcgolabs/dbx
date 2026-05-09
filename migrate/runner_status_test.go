package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
)

func TestRunnerStatusGo(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-status-go.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)
	migration2 := migrate.NewGoMigration("2", "add flag", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `ALTER TABLE sample ADD COLUMN flag INTEGER NOT NULL DEFAULT 0`)
		if execErr != nil {
			return fmt.Errorf("alter sample table: %w", execErr)
		}
		return nil
	}, nil)

	statuses, err := runner.StatusGo(ctx, migration1, migration2)
	require.NoError(t, err)
	require.Equal(t, 2, statuses.Len())
	for i := range statuses.Len() {
		status, ok := statuses.Get(i)
		require.True(t, ok)
		require.Equal(t, migrate.MigrationStatePending, status.State)
		require.Nil(t, status.AppliedAt)
	}

	_, err = runner.UpGo(ctx, migration1, migration2)
	require.NoError(t, err)

	statuses, err = runner.StatusGo(ctx, migration1, migration2)
	require.NoError(t, err)
	require.Equal(t, 2, statuses.Len())
	for i := range statuses.Len() {
		status, ok := statuses.Get(i)
		require.True(t, ok)
		require.Equal(t, migrate.MigrationStateApplied, status.State)
		require.NotNil(t, status.AppliedAt)
	}
}

func TestRunnerStatusAll(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-status-all.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql": &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
		},
		Dir: "sql",
	}

	report, err := runner.UpGo(ctx, migration1)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	_, err = runner.UpSQL(ctx, source)
	require.NoError(t, err)

	status, err := runner.StatusAll(ctx, []migrate.Migration{migration1}, &source)
	require.NoError(t, err)
	require.Equal(t, 1, status.Go.Len())
	require.Equal(t, 1, status.SQL.Len())
	for i := range status.Go.Len() {
		item, ok := status.Go.Get(i)
		require.True(t, ok)
		require.Equal(t, migrate.MigrationStateApplied, item.State)
	}
	for i := range status.SQL.Len() {
		item, ok := status.SQL.Get(i)
		require.True(t, ok)
		require.Equal(t, migrate.MigrationStateApplied, item.State)
	}
}

func TestRunnerStatusAllWithoutSQLSource(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-status-all-nosql.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)

	_, err := runner.UpGo(ctx, migration1)
	require.NoError(t, err)

	status, err := runner.StatusAll(ctx, []migrate.Migration{migration1}, nil)
	require.NoError(t, err)
	require.NotNil(t, status.Go)
	require.NotNil(t, status.SQL)
	require.Equal(t, 1, status.Go.Len())
	require.Equal(t, 0, status.SQL.Len())
}

func TestRunnerStatusSQLDetectsRepeatableOutdated(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-sql-status.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	baseSource := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
			"sql/R__refresh_cache.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		Dir: "sql",
	}
	_, err := runner.UpSQL(ctx, baseSource)
	require.NoError(t, err)

	nextSource := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
			"sql/R__refresh_cache.sql": &fstest.MapFile{Data: []byte("SELECT 2;\n")},
		},
		Dir: "sql",
	}

	statuses, err := runner.StatusSQL(ctx, nextSource)
	require.NoError(t, err)
	require.Equal(t, 2, statuses.Len())

	var repeatableStatus *migrate.MigrationStatus
	for i := range statuses.Len() {
		status, ok := statuses.Get(i)
		require.True(t, ok)
		if status.Kind == migrate.KindRepeatable {
			repeatableStatus = &status
			break
		}
	}
	require.NotNil(t, repeatableStatus)
	require.Equal(t, migrate.MigrationStateOutdated, repeatableStatus.State)
}

func TestRunnerPendingAll(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-pending-all.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
			"sql/R__refresh_cache.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		Dir: "sql",
	}

	pending, err := runner.PendingAll(ctx, []migrate.Migration{migration1}, &source)
	require.NoError(t, err)
	require.Equal(t, 1, pending.Go.Len())
	require.Equal(t, 2, pending.SQL.Len())
	require.Equal(t, "1", mustMigrationVersion(t, pending.Go, 0))
	require.Equal(t, "1", mustSQLMigrationVersion(t, pending.SQL, 0))
}

func TestRunnerPendingAllWithoutSQLSource(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-pending-all-nosql.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil)

	pending, err := runner.PendingAll(ctx, []migrate.Migration{migration1}, nil)
	require.NoError(t, err)
	require.NotNil(t, pending.Go)
	require.NotNil(t, pending.SQL)
	require.Equal(t, 1, pending.Go.Len())
	require.Equal(t, 0, pending.SQL.Len())
}

func mustMigrationVersion(t *testing.T, list *collectionx.List[migrate.Migration], index int) string {
	t.Helper()

	item, ok := list.Get(index)
	require.True(t, ok)
	return item.Version()
}

func mustSQLMigrationVersion(t *testing.T, list *collectionx.List[migrate.SQLMigration], index int) string {
	t.Helper()

	item, ok := list.Get(index)
	require.True(t, ok)
	return item.Version
}
