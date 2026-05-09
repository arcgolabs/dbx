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

func TestRunnerApplyAllUpAndDown(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-apply-all.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create logs", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE logs (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create logs table: %w", execErr)
		}
		return nil
	}, func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `DROP TABLE logs`)
		if execErr != nil {
			return fmt.Errorf("drop logs table: %w", execErr)
		}
		return nil
	})
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"sql/U1__create_users.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
		},
		Dir: "sql",
	}

	upReport, err := runner.ApplyAll(ctx, migrate.MigrationApplySpec{
		Direction:    migrate.DirectionUp,
		GoMigrations: []migrate.Migration{migration1},
		SQLSource:    &source,
	})
	require.NoError(t, err)
	require.Equal(t, 2, upReport.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
	require.True(t, sqliteTableExists(ctx, t, db, "users"))

	downReport, err := runner.ApplyAll(ctx, migrate.MigrationApplySpec{
		Direction:    migrate.DirectionDown,
		GoMigrations: []migrate.Migration{migration1},
		SQLSource:    &source,
	})
	require.NoError(t, err)
	require.Equal(t, 2, downReport.Applied.Len())
	require.False(t, sqliteTableExists(ctx, t, db, "logs"))
	require.False(t, sqliteTableExists(ctx, t, db, "users"))
}

func TestRunnerApplyAllWithTarget(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-apply-all-target.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create logs", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE logs (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create logs table: %w", execErr)
		}
		return nil
	}, func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `DROP TABLE logs`)
		if execErr != nil {
			return fmt.Errorf("drop logs table: %w", execErr)
		}
		return nil
	})
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
	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_orders.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")},
			"sql/U1__create_orders.sql":   &fstest.MapFile{Data: []byte("DROP TABLE orders;\n")},
			"sql/V2__create_products.sql": &fstest.MapFile{Data: []byte("CREATE TABLE products (id INTEGER PRIMARY KEY);\n")},
			"sql/U2__create_products.sql": &fstest.MapFile{Data: []byte("DROP TABLE products;\n")},
		},
		Dir: "sql",
	}

	upReport, err := runner.ApplyAll(ctx, migrate.MigrationApplySpec{
		Direction:    migrate.DirectionUp,
		Target:       &migrate.MigrationTarget{Version: 1},
		GoMigrations: []migrate.Migration{migration1, migration2},
		SQLSource:    &source,
	})
	require.NoError(t, err)
	require.Equal(t, 2, upReport.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
	require.True(t, sqliteTableExists(ctx, t, db, "orders"))
	require.False(t, sqliteTableExists(ctx, t, db, "users"))
	require.False(t, sqliteTableExists(ctx, t, db, "products"))
}

func TestRunnerValidateApplyAll(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-validate-apply-all.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration := migrate.NewGoMigration("1", "create logs", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE logs (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create logs table: %w", execErr)
		}
		return nil
	}, nil)

	err := runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.DirectionUp,
		GoMigrations: []migrate.Migration{migration},
		Target:       &migrate.MigrationTarget{Version: 1},
	})
	require.NoError(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.DirectionDown,
		GoMigrations: []migrate.Migration{migration},
		Target:       &migrate.MigrationTarget{Version: 0},
	})
	require.NoError(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.DirectionUp,
		GoMigrations: []migrate.Migration{migration},
		Target:       &migrate.MigrationTarget{Version: 0},
	})
	require.Error(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.DirectionDown,
		GoMigrations: []migrate.Migration{migration},
		Target:       &migrate.MigrationTarget{Version: -1},
	})
	require.Error(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.Direction("INVALID"),
		GoMigrations: []migrate.Migration{migration},
		Target:       nil,
	})
	require.NoError(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction:    migrate.DirectionUp,
		GoMigrations: []migrate.Migration{migration},
		Target:       &migrate.MigrationTarget{Version: 1},
		SQLSource: &migrate.FileSource{
			FS: fstest.MapFS{
				"sql/V1__noop.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
			},
			Dir: "sql",
		},
	})
	require.NoError(t, err)

	err = runner.ValidateApplyAll(migrate.MigrationApplySpec{})
	require.NoError(t, err)

	_, err = runner.ApplyAll(ctx, migrate.MigrationApplySpec{
		Direction: migrate.DirectionUp,
		Target:    &migrate.MigrationTarget{Version: 0},
	})
	require.Error(t, err)

	var nilRunner *migrate.Runner
	err = nilRunner.ValidateApplyAll(migrate.MigrationApplySpec{
		Direction: migrate.DirectionUp,
	})
	require.Error(t, err)
}
