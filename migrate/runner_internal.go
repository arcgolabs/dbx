package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
)

const timeLayout = "2006-01-02T15:04:05.999999999Z07:00"

func appliedRecordKey(kind Kind, version, description string) string {
	return string(kind) + "\x1f" + version + "\x1f" + description
}

func indexAppliedRecords(records collectionx.List[AppliedRecord]) map[string]AppliedRecord {
	return collectionx.AssociateList[AppliedRecord, string, AppliedRecord](records, func(_ int, record AppliedRecord) (string, AppliedRecord) {
		return appliedRecordKey(record.Kind, record.Version, record.Description), record
	}).All()
}

func checksumGoMigration(migration Migration) string {
	return checksumString("go|" + migration.Version() + "|" + migration.Description())
}

func checksumSQLMigration(migration SQLMigration, upSQL, downSQL string) string {
	return checksumString(strings.Join([]string{
		string(kindForSQLMigration(migration)),
		migration.Version,
		migration.Description,
		upSQL,
		downSQL,
	}, "\n--dbx-migrate--\n"))
}

func checksumString(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func historyTableDDL(d dialect.Dialect, table string) string {
	q := d.QuoteIdent
	return "CREATE TABLE IF NOT EXISTS " + q(table) + " (" +
		q("version") + " VARCHAR(255) NOT NULL, " +
		q("description") + " VARCHAR(255) NOT NULL, " +
		q("kind") + " VARCHAR(32) NOT NULL, " +
		q("checksum") + " VARCHAR(128) NOT NULL, " +
		q("success") + " BOOLEAN NOT NULL, " +
		q("applied_at") + " VARCHAR(64) NOT NULL, " +
		"PRIMARY KEY (" + q("version") + ", " + q("kind") + ", " + q("description") + "))"
}

func appliedRecordsSQL(d dialect.Dialect, table string) string {
	q := d.QuoteIdent
	return "SELECT " + q("version") + ", " + q("description") + ", " + q("kind") + ", " + q("applied_at") + ", " + q("checksum") + ", " + q("success") +
		" FROM " + q(table) +
		" ORDER BY " + q("applied_at") + ", " + q("version") + ", " + q("description")
}

func historyRowsForStatusSQL(d dialect.Dialect, table string) string {
	q := d.QuoteIdent
	return "SELECT " + q("version") + ", " + q("description") + ", " + q("kind") + ", " + q("applied_at") + ", " + q("success") +
		" FROM " + q(table) +
		" WHERE " + q("success") + " = " + truthyLiteral(d) +
		" AND " + q("kind") + " <> " + d.BindVar(1) +
		" ORDER BY " + q("applied_at") + " DESC, " + q("version") + " DESC, " + q("description") + " DESC"
}

func specificAppliedMigrationSQL(d dialect.Dialect, table string) string {
	q := d.QuoteIdent
	return "SELECT " + q("applied_at") + ", " + q("success") +
		" FROM " + q(table) +
		" WHERE " + q("version") + " = " + d.BindVar(1) +
		" AND " + q("kind") + " = " + d.BindVar(2) +
		" AND " + q("description") + " = " + d.BindVar(3) +
		" ORDER BY " + q("applied_at") + " DESC"
}

func truthyLiteral(d dialect.Dialect) string {
	switch strings.ToLower(strings.TrimSpace(d.Name())) {
	case "mysql":
		return "1"
	default:
		return "TRUE"
	}
}

func replaceAppliedRecord(ctx context.Context, tx *sql.Tx, d dialect.Dialect, table string, record AppliedRecord) error {
	return replaceAppliedRecordOnConn(ctx, tx, d, table, record)
}

func replaceAppliedRecordOnConn(ctx context.Context, conn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, d dialect.Dialect, table string, record AppliedRecord) error {
	q := d.QuoteIdent
	deleteSQL := "DELETE FROM " + q(table) +
		" WHERE " + q("version") + " = " + d.BindVar(1) +
		" AND " + q("kind") + " = " + d.BindVar(2) +
		" AND " + q("description") + " = " + d.BindVar(3)
	if _, err := conn.ExecContext(ctx, deleteSQL, record.Version, string(record.Kind), record.Description); err != nil {
		return fmt.Errorf("dbx/migrate: delete applied record %s/%s: %w", record.Version, record.Description, err)
	}

	insertSQL := "INSERT INTO " + q(table) +
		" (" + q("version") + ", " + q("description") + ", " + q("kind") + ", " + q("checksum") + ", " + q("success") + ", " + q("applied_at") + ")" +
		" VALUES (" + d.BindVar(1) + ", " + d.BindVar(2) + ", " + d.BindVar(3) + ", " + d.BindVar(4) + ", " + d.BindVar(5) + ", " + d.BindVar(6) + ")"
	_, err := conn.ExecContext(ctx, insertSQL,
		record.Version,
		record.Description,
		string(record.Kind),
		record.Checksum,
		record.Success,
		record.AppliedAt.UTC().Format(timeLayout),
	)
	if err != nil {
		return fmt.Errorf("dbx/migrate: insert applied record %s/%s: %w", record.Version, record.Description, err)
	}
	return nil
}

func appliedRecordForVersion(items collectionx.List[AppliedRecord], record AppliedRecord) (AppliedRecord, error) {
	found, ok := items.FirstWhere(func(_ int, item AppliedRecord) bool {
		return item.Kind == record.Kind && item.Version == record.Version && item.Description == record.Description
	}).Get()
	if !ok {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: applied record not found for version %s", record.Version)
	}
	return found, nil
}
