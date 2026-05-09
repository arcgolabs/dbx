package migrate_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
)

func TestRunnerUpGoCreatesHistoryAndAppliesMigration(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	report, err := runner.UpGo(ctx, migrate.NewGoMigration("1", "create sample", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE sample (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create sample table: %w", execErr)
		}
		return nil
	}, nil))
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	reportItem, ok := report.Applied.GetFirst()
	require.True(t, ok)
	require.Equal(t, "1", reportItem.Version)
	require.Equal(t, migrate.KindGo, reportItem.Kind)

	applied, err := runner.Applied(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, applied.Len())
	appliedItem, ok := applied.GetFirst()
	require.True(t, ok)
	require.Equal(t, "1", appliedItem.Version)
	require.Equal(t, migrate.KindGo, appliedItem.Kind)
	require.True(t, appliedItem.Success)

	require.True(t, sqliteTableExists(ctx, t, db, "sample"))
	require.True(t, sqliteTableExists(ctx, t, db, "schema_history"))
}

// Matches migrate.checksumString / checksumSQLMigration (repeatable migrations, trimmed up SQL like readSQLFile).
func repeatableSQLChecksumForTest(version, description, upSQL, downSQL string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"repeatable",
		version,
		description,
		upSQL,
		downSQL,
	}, "\n--dbx-migrate--\n")))
	return hex.EncodeToString(sum[:])
}

func TestRunnerPendingSQLTracksRepeatableChecksum(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})
	require.NoError(t, runner.EnsureHistory(ctx))
	oldUp := strings.TrimSpace("SELECT 2;\n")
	chk := repeatableSQLChecksumForTest("", "refresh cache", oldUp, "")
	appliedAt := time.Date(2026, 3, 19, 22, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.999999999Z07:00")
	if _, err := db.ExecContext(ctx, `INSERT INTO "schema_history" ("version", "description", "kind", "checksum", "success", "applied_at") VALUES (?, ?, ?, ?, ?, ?)`,
		"", "refresh cache", "repeatable", chk, true, appliedAt,
	); err != nil {
		t.Fatalf("insert schema_history: %v", err)
	}

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/R__refresh_cache.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		Dir: "sql",
	}
	pending, err := runner.PendingSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 1, pending.Len())
	item, ok := pending.GetFirst()
	require.True(t, ok)
	require.True(t, item.Repeatable)
}

func TestRunnerPendingGoReturnsCollectionxList(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	pending, err := runner.PendingGo(ctx, migrate.NewGoMigration("1", "create sample", func(context.Context, *sql.Tx) error {
		return nil
	}, nil))
	require.NoError(t, err)
	require.Equal(t, 1, pending.Len())
	item, ok := pending.GetFirst()
	require.True(t, ok)
	require.Equal(t, "1", item.Version())
}

func TestRunnerUpSQLAppliesVersionedFiles(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_logs.sql": &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INTEGER PRIMARY KEY);\n")},
		},
		Dir: "sql",
	}

	report, err := runner.UpSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 1, report.Applied.Len())
	reportItem, ok := report.Applied.GetFirst()
	require.True(t, ok)
	require.Equal(t, "1", reportItem.Version)
	require.Equal(t, migrate.KindSQL, reportItem.Kind)

	applied, err := runner.Applied(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, applied.Len())
	appliedItem, ok := applied.GetFirst()
	require.True(t, ok)
	require.Equal(t, "1", appliedItem.Version)
	require.Equal(t, migrate.KindSQL, appliedItem.Kind)
	require.True(t, appliedItem.Success)

	require.True(t, sqliteTableExists(ctx, t, db, "logs"))
	require.True(t, sqliteTableExists(ctx, t, db, "schema_history"))
}

func TestRunnerUpSQLAppliesGenericAndMatchingDialectFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-sql-dialect.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_generic.sql":                     &fstest.MapFile{Data: []byte("CREATE TABLE generic (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_sqlite_specific_tbl__sqlite.sql": &fstest.MapFile{Data: []byte("CREATE TABLE dialect_specific_table (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_mysql_specific__mysql.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE mysql_specific_table (id INTEGER PRIMARY KEY);\n")},
			"sql/V3__create_sqlite_single_sqlite.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE dialect_single_table (id INTEGER PRIMARY KEY);\n")},
		},
		Dir: "sql",
	}

	report, err := runner.UpSQL(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 3, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "generic"))
	require.True(t, sqliteTableExists(ctx, t, db, "dialect_specific_table"))
	require.True(t, sqliteTableExists(ctx, t, db, "dialect_single_table"))
	require.False(t, sqliteTableExists(ctx, t, db, "mysql_specific_table"))
}

func TestRunnerUpSQLForUsesCallTimeDialect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-sql-calltime-dialect.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_generic.sql":               &fstest.MapFile{Data: []byte("CREATE TABLE generic_runtime (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_sqlite_runtime_sqlite.sql": &fstest.MapFile{Data: []byte("CREATE TABLE dialect_runtime (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_mysql_runtime_mysql.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE mysql_runtime (id INTEGER PRIMARY KEY);\n")},
		},
		Dir:      "sql",
		Database: migrate.DialectSQLite,
	}

	report, err := runner.UpSQLFor(ctx, migrate.DialectMySQL, source)
	require.NoError(t, err)
	require.Equal(t, 2, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "generic_runtime"))
	require.False(t, sqliteTableExists(ctx, t, db, "dialect_runtime"))
	require.True(t, sqliteTableExists(ctx, t, db, "mysql_runtime"))
}

func TestRunnerUpGoAppliesMatchingDialectOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-go-dialect.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	migration1 := migrate.NewGoMigration("1", "create generic", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE generic_go (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create generic_go table: %w", execErr)
		}
		return nil
	}, nil)
	migration2 := migrate.NewGoMigration("2", "create sqlite", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE migration_sqlite (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create migration_sqlite table: %w", execErr)
		}
		return nil
	}, nil, migrate.DialectSQLite)
	migration3 := migrate.NewGoMigration("3", "create mysql", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE mysql_migration (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create mysql_migration table: %w", execErr)
		}
		return nil
	}, nil, migrate.DialectMySQL)

	_, err := runner.UpGo(ctx, migration1, migration2, migration3)
	require.NoError(t, err)
	require.True(t, sqliteTableExists(ctx, t, db, "generic_go"))
	require.True(t, sqliteTableExists(ctx, t, db, "migration_sqlite"))
	require.False(t, sqliteTableExists(ctx, t, db, "mysql_migration"))
}

func TestMigrationFacadesUseCallTimeDialect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openSQLiteRunnerDB(t, filepath.Join(t.TempDir(), "runner-facade-dialect.db"))
	runner := migrate.NewRunner(db, testDialect{}, migrate.RunnerOptions{ValidateHash: true})

	source := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_sql_generic.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE sql_generic (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_sqlite_only_sqlite.sql": &fstest.MapFile{Data: []byte("CREATE TABLE sql_sqlite_only (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__create_mysql_only_mysql.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE sql_mysql_only (id INTEGER PRIMARY KEY);\n")},
		},
		Dir:      "sql",
		Database: migrate.DialectSQLite,
	}

	report, err := migrate.SQL(runner).ForDialect(migrate.DialectMySQL).Up(ctx, source)
	require.NoError(t, err)
	require.Equal(t, 2, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "sql_generic"))
	require.False(t, sqliteTableExists(ctx, t, db, "sql_sqlite_only"))
	require.True(t, sqliteTableExists(ctx, t, db, "sql_mysql_only"))

	goGeneric := migrate.NewGoMigration("3", "create go generic", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE go_generic (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create go_generic table: %w", execErr)
		}
		return nil
	}, nil)
	goSQLite := migrate.NewGoMigration("4", "create go sqlite", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE go_sqlite_only (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create go_sqlite_only table: %w", execErr)
		}
		return nil
	}, nil, migrate.DialectSQLite)
	goMySQL := migrate.NewGoMigration("5", "create go mysql", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, `CREATE TABLE go_mysql_only (id INTEGER PRIMARY KEY)`)
		if execErr != nil {
			return fmt.Errorf("create go_mysql_only table: %w", execErr)
		}
		return nil
	}, nil, migrate.DialectMySQL)

	report, err = migrate.Go(runner).ForDialect(migrate.DialectMySQL).Up(ctx, goGeneric, goSQLite, goMySQL)
	require.NoError(t, err)
	require.Equal(t, 2, report.Applied.Len())
	require.True(t, sqliteTableExists(ctx, t, db, "go_generic"))
	require.False(t, sqliteTableExists(ctx, t, db, "go_sqlite_only"))
	require.True(t, sqliteTableExists(ctx, t, db, "go_mysql_only"))
}
