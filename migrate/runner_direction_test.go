package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
)

func TestRunnerUpGoToAndDownGoTo(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-go-to.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create logs", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE logs (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create logs table: %w", execErr)
		}
		return nil
	}, nil)
	migration2 := migrate.NewGoMigration("2", "create users", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create users table: %w", execErr)
		}
		return nil
	}, func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `DROP TABLE users`)
		if execErr != nil {
			return fmt.Errorf("drop users table: %w", execErr)
		}
		return nil
	})

	report, err := runner.UpGoTo(ctx, 1, migration1, migration2)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
	require.False(t, sqliteTableExists(ctx, t, db, "users"))

	report, err = runner.UpGoTo(ctx, 2, migration1, migration2)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "users"))

	report, err = runner.DownGoTo(ctx, 1, migration1, migration2)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.False(t, sqliteTableExists(ctx, t, db, "users"))
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))

	migrationCleanup := migrate.NewGoMigration("3", "create cleanup table", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE tmp_cleanup (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create tmp cleanup table: %w", execErr)
		}
		return nil
	}, func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `DROP TABLE tmp_cleanup`)
		if execErr != nil {
			return fmt.Errorf("drop tmp cleanup table: %w", execErr)
		}
		return nil
	})

	_, err = runner.UpGo(ctx, migrationCleanup)
	require.NoError(t, err)
	require.True(t, sqliteTableExists(ctx, t, db, "tmp_cleanup"))

	report, err = runner.DownGoTo(ctx, 0, migration1, migration2, migrationCleanup)
	require.NoError(t, err)
	require.Equal(t, 2, report.Applied.Len())
	require.False(t, sqliteTableExists(ctx, t, db, "tmp_cleanup"))
	require.False(t, sqliteTableExists(ctx, t, db, "users"))
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
}

func TestRunnerUpGoToAndDownGoToInvalidTarget(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-go-to-invalid.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	report, err := runner.UpGoTo(ctx, 0)
	require.Error(t, err)
	require.Equal(t, 0, report.Applied.Len())

	report, err = runner.DownGoTo(ctx, -1)
	require.Error(t, err)
	require.Equal(t, 0, report.Applied.Len())
}

func TestRunnerUpSQLToAndDownSQLTo(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-sql-to.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"sql/U2__create_users.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
		},
		Dir: "sql",
	}

	report, err := runner.UpSQLTo(ctx, 1, source)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
	require.False(t, sqliteTableExists(ctx, t, db, "users"))

	report, err = runner.UpSQLTo(ctx, 2, source)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "users"))

	report, err = runner.DownSQLTo(ctx, 1, source)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	require.False(t, sqliteTableExists(ctx, t, db, "users"))
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
}

func TestRunnerUpSQLToAndDownSQLToInvalidTarget(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-sql-to-invalid.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{FS: fstest.MapFS{}, Dir: "sql"}

	_, err := runner.UpSQLTo(ctx, 0, source)
	require.Error(t, err)

	_, err = runner.DownSQLTo(ctx, -1, source)
	require.Error(t, err)
}
