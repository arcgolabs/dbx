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

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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
	item, ok := list.Get(index)
	require.True(t, ok)
	return item.Version()
}

func mustSQLMigrationVersion(t *testing.T, list *collectionx.List[migrate.SQLMigration], index int) string {
	item, ok := list.Get(index)
	require.True(t, ok)
	return item.Version
}
