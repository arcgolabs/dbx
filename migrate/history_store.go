package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	goosedatabase "github.com/pressly/goose/v3/database"
)

type historyStore struct {
	tableName     string
	dialect       dialect.Dialect
	metaByVersion collectionx.Map[int64, AppliedRecord]
}

func newHistoryStore(d dialect.Dialect, tableName string, metaByVersion collectionx.Map[int64, AppliedRecord]) *historyStore {
	return &historyStore{
		tableName:     tableName,
		dialect:       d,
		metaByVersion: metaByVersion,
	}
}

func (s *historyStore) Tablename() string {
	return s.tableName
}

func (s *historyStore) CreateVersionTable(ctx context.Context, db goosedatabase.DBTxConn) error {
	_, err := db.ExecContext(ctx, historyTableDDL(s.dialect, s.tableName))
	if err != nil {
		return fmt.Errorf("dbx/migrate: create history table %q: %w", s.tableName, err)
	}
	return nil
}

func (s *historyStore) TableExists(ctx context.Context, db goosedatabase.DBTxConn) (bool, error) {
	query, err := historyTableExistsSQL(s.dialect)
	if err != nil {
		return false, err
	}
	var exists bool
	if err := db.QueryRowContext(ctx, query, s.tableName).Scan(&exists); err != nil {
		return false, fmt.Errorf("dbx/migrate: query history table %q existence: %w", s.tableName, err)
	}
	return exists, nil
}

func (s *historyStore) Insert(ctx context.Context, db goosedatabase.DBTxConn, req goosedatabase.InsertRequest) error {
	if req.Version == 0 {
		return nil
	}
	record, ok := s.metaByVersion.Get(req.Version)
	if !ok {
		return fmt.Errorf("dbx/migrate: migration metadata not found for version %d", req.Version)
	}
	record.AppliedAt = time.Now().UTC()
	record.Success = true
	return replaceAppliedRecordOnConn(ctx, db, s.dialect, s.tableName, record)
}

func (s *historyStore) Delete(ctx context.Context, db goosedatabase.DBTxConn, version int64) error {
	if version == 0 {
		return nil
	}
	record, ok := s.metaByVersion.Get(version)
	if !ok {
		return fmt.Errorf("dbx/migrate: migration metadata not found for version %d", version)
	}
	q := s.dialect.QuoteIdent
	deleteSQL := "DELETE FROM " + q(s.tableName) +
		" WHERE " + q("version") + " = " + s.dialect.BindVar(1) +
		" AND " + q("kind") + " = " + s.dialect.BindVar(2) +
		" AND " + q("description") + " = " + s.dialect.BindVar(3)
	_, err := db.ExecContext(ctx, deleteSQL, record.Version, string(record.Kind), record.Description)
	if err != nil {
		return fmt.Errorf("dbx/migrate: delete history record %s/%s: %w", record.Version, record.Description, err)
	}
	return nil
}

func (s *historyStore) GetMigration(ctx context.Context, db goosedatabase.DBTxConn, version int64) (*goosedatabase.GetMigrationResult, error) {
	if version == 0 {
		return &goosedatabase.GetMigrationResult{IsApplied: true}, nil
	}
	record, ok := s.metaByVersion.Get(version)
	if !ok {
		return nil, goosedatabase.ErrVersionNotFound
	}

	query := specificAppliedMigrationSQL(s.dialect, s.tableName)
	var (
		appliedAt string
		success   bool
	)
	if err := db.QueryRowContext(ctx, query, record.Version, string(record.Kind), record.Description).Scan(&appliedAt, &success); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, goosedatabase.ErrVersionNotFound
		}
		return nil, fmt.Errorf("dbx/migrate: query history record %s/%s: %w", record.Version, record.Description, err)
	}
	timestamp, err := time.Parse(timeLayout, appliedAt)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: parse applied_at: %w", err)
	}
	return &goosedatabase.GetMigrationResult{Timestamp: timestamp, IsApplied: success}, nil
}

func (s *historyStore) GetLatestVersion(ctx context.Context, db goosedatabase.DBTxConn) (int64, error) {
	items, err := s.ListMigrations(ctx, db)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	var maxItem *goosedatabase.ListMigrationsResult
	collectionx.NewList[*goosedatabase.ListMigrationsResult](items...).Range(func(_ int, candidate *goosedatabase.ListMigrationsResult) bool {
		if maxItem == nil || candidate.Version > maxItem.Version {
			maxItem = candidate
		}
		return true
	})
	if maxItem == nil {
		return 0, nil
	}
	return maxItem.Version, nil
}

func (s *historyStore) ListMigrations(ctx context.Context, db goosedatabase.DBTxConn) (_ []*goosedatabase.ListMigrationsResult, resultErr error) {
	rows, err := s.queryHistoryRows(ctx, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = closeSQLRows(rows, "history rows", resultErr)
	}()

	items, err := s.collectListMigrations(rows)
	if err != nil {
		return nil, err
	}
	return ensureListMigrationsResult(items), nil
}

func (s *historyStore) listMigrationResult(rows *sql.Rows) (*goosedatabase.ListMigrationsResult, bool, error) {
	var (
		version     string
		description string
		kind        string
		appliedAt   string
		success     bool
	)
	if err := rows.Scan(&version, &description, &kind, &appliedAt, &success); err != nil {
		return nil, false, fmt.Errorf("dbx/migrate: scan history row: %w", err)
	}
	parsed, err := parseNumericVersion(version)
	if err != nil {
		return nil, false, err
	}
	record, ok := s.metaByVersion.Get(parsed)
	if !ok || record.Kind != Kind(kind) || record.Description != description {
		return nil, false, nil
	}
	return &goosedatabase.ListMigrationsResult{Version: parsed, IsApplied: true}, true, nil
}

func (s *historyStore) queryHistoryRows(ctx context.Context, db goosedatabase.DBTxConn) (*sql.Rows, error) {
	rows, err := db.QueryContext(ctx, historyRowsForStatusSQL(s.dialect, s.tableName), string(KindRepeatable))
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: query history rows: %w", err)
	}
	return rows, nil
}

func (s *historyStore) collectListMigrations(rows *sql.Rows) ([]*goosedatabase.ListMigrationsResult, error) {
	items := collectionx.NewList[*goosedatabase.ListMigrationsResult]()
	for rows.Next() {
		result, ok, scanErr := s.listMigrationResult(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		if ok {
			items.Add(result)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbx/migrate: iterate history rows: %w", err)
	}
	return items.Values(), nil
}

func ensureListMigrationsResult(items []*goosedatabase.ListMigrationsResult) []*goosedatabase.ListMigrationsResult {
	if len(items) > 0 {
		return items
	}
	return []*goosedatabase.ListMigrationsResult{{Version: 0, IsApplied: true}}
}

func historyTableExistsSQL(d dialect.Dialect) (string, error) {
	switch d.Name() {
	case "sqlite":
		return "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)", nil
	case "postgres":
		return "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = " + d.BindVar(1) + ")", nil
	case "mysql":
		return "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)", nil
	default:
		return "", errors.ErrUnsupported
	}
}
