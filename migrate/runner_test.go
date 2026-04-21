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
