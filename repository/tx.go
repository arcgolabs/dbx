package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/arcgolabs/dbx"
)

// InTx executes fn within a database transaction and binds the repository to it.
func (r *Base[E, S]) InTx(ctx context.Context, opts *sql.TxOptions, fn func(tx *dbx.Tx, repo *Base[E, S]) error) error {
	if r == nil || r.db == nil {
		return dbx.ErrNilDB
	}
	if fn == nil {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	txRepo := &Base[E, S]{
		db:                  r.db,
		session:             tx,
		schema:              r.schema,
		mapper:              r.mapper,
		byIDNotFoundAsError: r.byIDNotFoundAsError,
	}
	runErr := fn(tx, txRepo)
	if runErr != nil {
		if rollbackErr := tx.RollbackContext(ctx); rollbackErr != nil {
			return errors.Join(runErr, fmt.Errorf("rollback tx: %w", rollbackErr))
		}
		return runErr
	}
	if err := tx.CommitContext(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
