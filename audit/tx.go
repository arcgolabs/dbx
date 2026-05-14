package audit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/arcgolabs/dbx"
)

// InTx runs fn in a transaction and adds a revision scope to ctx.
func InTx(ctx context.Context, db *dbx.DB, opts *sql.TxOptions, fn func(context.Context, *dbx.Tx) error) error {
	if db == nil {
		return dbx.ErrNilDB
	}
	if fn == nil {
		return nil
	}
	ctx = WithRevisionScope(ctx)
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	runErr := fn(ctx, tx)
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
