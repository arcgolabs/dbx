package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/DaiYuANg/arcgo/collectionx"
)

// EnsureHistory creates the migration history table when it does not yet exist.
func (r *Runner) EnsureHistory(ctx context.Context) error {
	if r == nil || r.db == nil {
		return sql.ErrConnDone
	}
	return newHistoryStore(r.dialect, r.options.HistoryTable, collectionx.NewMap[int64, AppliedRecord]()).CreateVersionTable(ctx, r.db)
}

// Applied returns all applied migration records from the history table.
func (r *Runner) Applied(ctx context.Context) (_ collectionx.List[AppliedRecord], resultErr error) {
	if r == nil || r.db == nil {
		return nil, sql.ErrConnDone
	}
	if err := r.EnsureHistory(ctx); err != nil {
		return nil, err
	}

	rows, err := r.queryAppliedRows(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = closeSQLRows(rows, "applied rows", resultErr)
	}()

	return collectAppliedRecords(rows)
}

func scanAppliedRecord(rows *sql.Rows) (AppliedRecord, error) {
	var (
		record      AppliedRecord
		kind        string
		appliedAt   string
		successFlag bool
	)
	if err := rows.Scan(&record.Version, &record.Description, &kind, &appliedAt, &record.Checksum, &successFlag); err != nil {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: scan applied row: %w", err)
	}

	parsedTime, err := time.Parse(timeLayout, appliedAt)
	if err != nil {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: parse applied_at: %w", err)
	}
	record.Kind = Kind(kind)
	record.AppliedAt = parsedTime
	record.Success = successFlag
	return record, nil
}

func (r *Runner) queryAppliedRows(ctx context.Context) (*sql.Rows, error) {
	rows, err := r.db.QueryContext(ctx, appliedRecordsSQL(r.dialect, r.options.HistoryTable))
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: query applied records: %w", err)
	}
	return rows, nil
}

func collectAppliedRecords(rows *sql.Rows) (collectionx.List[AppliedRecord], error) {
	items := collectionx.NewListWithCapacity[AppliedRecord](8)
	for rows.Next() {
		record, scanErr := scanAppliedRecord(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items.Add(record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbx/migrate: iterate applied rows: %w", err)
	}
	return items, nil
}

func closeSQLRows(rows *sql.Rows, description string, currentErr error) error {
	if currentErr != nil {
		if closeErr := rows.Close(); closeErr != nil {
			return errors.Join(currentErr, fmt.Errorf("dbx/migrate: close %s: %w", description, closeErr))
		}
		return currentErr
	}
	if closeErr := rows.Close(); closeErr != nil {
		return fmt.Errorf("dbx/migrate: close %s: %w", description, closeErr)
	}
	return nil
}
