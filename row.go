package dbx

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// Row wraps a single-row query so hook observation can complete after Scan.
type Row struct {
	ctx     context.Context
	observe runtimeObserver
	event   *HookEvent

	rows *sql.Rows
	err  error

	mu        sync.Mutex
	consumed  bool
	resultErr error
	afterOnce sync.Once
}

func errorRow(err error) *Row {
	return &Row{err: err}
}

func observedRow(ctx context.Context, observe runtimeObserver, event *HookEvent, rows *sql.Rows) *Row {
	return &Row{ctx: ctx, observe: observe, event: event, rows: rows}
}

func (r *Row) Scan(dest ...any) error {
	if r == nil {
		return ErrNilRow
	}

	r.mu.Lock()
	if r.consumed {
		err := r.resultErr
		r.mu.Unlock()
		return err
	}
	r.consumed = true
	rows := r.rows
	presetErr := r.err
	r.mu.Unlock()

	err := presetErr
	if err == nil {
		err = scanSingleRow(rows, dest...)
	}

	r.mu.Lock()
	r.resultErr = err
	r.mu.Unlock()

	r.afterOnce.Do(func() {
		if r.event != nil {
			r.event.Err = err
			r.observe.after(r.ctx, r.event)
		}
	})
	return err
}

func scanSingleRow(rows *sql.Rows, dest ...any) error {
	if rows == nil {
		return sql.ErrNoRows
	}
	if !rows.Next() {
		err := errors.Join(rowsIterError(rows), closeRows(rows))
		if err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := wrapDBError("scan row", rows.Scan(dest...))
	closeErr := closeRows(rows)
	rowsErr := rowsIterError(rows)
	if err != nil {
		return errors.Join(err, rowsErr, closeErr)
	}
	if closeErr != nil {
		return errors.Join(rowsErr, closeErr)
	}

	return rowsErr
}
