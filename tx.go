package dbx

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/relationruntime"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"

	"github.com/samber/oops"
)

type Tx struct {
	raw         *sql.Tx
	dialect     dialect.Dialect
	observe     runtimeObserver
	relation    *relationruntime.Runtime
	idGenerator idgen.Generator
	nodeID      uint16
}

func (tx *Tx) SQLTx() *sql.Tx {
	return tx.raw
}

func (tx *Tx) Dialect() dialect.Dialect {
	return tx.dialect
}

func (tx *Tx) Bound(rawSQL string, args ...any) sqlstmt.Bound {
	return sqlstmt.Bound{SQL: rawSQL, Args: collectionx.NewList[any](args...)}
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.queryContext(ctx, "", query, args...)
}

func (tx *Tx) queryContext(ctx context.Context, statement, query string, args ...any) (*sql.Rows, error) {
	if tx == nil {
		return nil, oops.In("dbx").
			With("op", "query", "statement", statement, "scope", "tx").
			Wrapf(ErrNilDB, "validate transaction")
	}
	if tx.raw == nil {
		return nil, oops.In("dbx").
			With("op", "query", "statement", statement, "scope", "tx").
			Wrapf(ErrNilSQLDB, "validate sql tx")
	}

	return observedQueryContext(ctx, tx.observe, statement, query, args, tx.raw.QueryContext)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.execContext(ctx, "", query, args...)
}

func (tx *Tx) execContext(ctx context.Context, statement, query string, args ...any) (sql.Result, error) {
	if tx == nil {
		return nil, oops.In("dbx").
			With("op", "exec", "statement", statement, "scope", "tx").
			Wrapf(ErrNilDB, "validate transaction")
	}
	if tx.raw == nil {
		return nil, oops.In("dbx").
			With("op", "exec", "statement", statement, "scope", "tx").
			Wrapf(ErrNilSQLDB, "validate sql tx")
	}

	return observedExecContext(ctx, tx.observe, statement, query, args, tx.raw.ExecContext)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	if tx == nil {
		return errorRow(oops.In("dbx").
			With("op", "query_row", "scope", "tx").
			Wrapf(ErrNilDB, "validate transaction"))
	}
	if tx.raw == nil {
		return errorRow(oops.In("dbx").
			With("op", "query_row", "scope", "tx").
			Wrapf(ErrNilSQLDB, "validate sql tx"))
	}
	ctx, event, err := tx.observe.before(ctx, HookEvent{Operation: OperationQueryRow, SQL: query, Args: collectionx.NewList[any](args...)})
	if err != nil {
		tx.observe.after(ctx, event)
		return errorRow(err)
	}
	rows, queryErr := tx.raw.QueryContext(ctx, query, args...)
	if queryErr != nil {
		event.Err = oops.In("dbx").
			With("op", "query_row", "scope", "tx").
			Wrapf(queryErr, "query row")
		tx.observe.after(ctx, event)
		return errorRow(event.Err)
	}
	return observedRow(ctx, tx.observe, event, rows)
}

func (tx *Tx) QueryBoundContext(ctx context.Context, bound sqlstmt.Bound) (*sql.Rows, error) {
	return tx.queryContext(ctx, bound.Name, bound.SQL, bound.Args.Values()...)
}

func (tx *Tx) ExecBoundContext(ctx context.Context, bound sqlstmt.Bound) (sql.Result, error) {
	return tx.execContext(ctx, bound.Name, bound.SQL, bound.Args.Values()...)
}

// Commit commits the transaction using a background context.
func (tx *Tx) Commit() error {
	return tx.CommitContext(context.Background())
}

// CommitContext commits the transaction using the provided context.
func (tx *Tx) CommitContext(ctx context.Context) error {
	if tx == nil || tx.raw == nil {
		return oops.In("dbx").
			With("op", "commit_tx").
			Wrapf(ErrNilSQLDB, "validate sql tx")
	}
	ctx, err := requireContext(ctx, "commit transaction")
	if err != nil {
		return err
	}
	ctx, event, err := tx.observe.before(ctx, HookEvent{Operation: OperationCommitTx})
	if err != nil {
		tx.observe.after(ctx, event)
		return err
	}
	commitErr := tx.raw.Commit()
	event.Err = wrapDBError("commit transaction", commitErr)
	tx.observe.after(ctx, event)
	return event.Err
}

// Rollback rolls the transaction back using a background context.
func (tx *Tx) Rollback() error {
	return tx.RollbackContext(context.Background())
}

// RollbackContext rolls the transaction back using the provided context.
func (tx *Tx) RollbackContext(ctx context.Context) error {
	if tx == nil || tx.raw == nil {
		return oops.In("dbx").
			With("op", "rollback_tx").
			Wrapf(ErrNilSQLDB, "validate sql tx")
	}
	ctx, err := requireContext(ctx, "rollback transaction")
	if err != nil {
		return err
	}
	ctx, event, err := tx.observe.before(ctx, HookEvent{Operation: OperationRollbackTx})
	if err != nil {
		tx.observe.after(ctx, event)
		return err
	}
	rollbackErr := tx.raw.Rollback()
	event.Err = wrapDBError("rollback transaction", rollbackErr)
	tx.observe.after(ctx, event)
	return event.Err
}

func (tx *Tx) SQL() *sqlexec.Executor {
	return sqlexec.New(tx)
}

func (tx *Tx) Logger() *slog.Logger {
	return tx.observe.logger
}

func (tx *Tx) Debug() bool {
	return tx.observe.debug
}

func (tx *Tx) IDGenerator() idgen.Generator {
	if tx == nil {
		return nil
	}
	return tx.idGenerator
}

func (tx *Tx) NodeID() uint16 {
	if tx == nil {
		return 0
	}
	return tx.nodeID
}

// RelationRuntime returns the relation load runtime for this Tx.
func (tx *Tx) RelationRuntime() *relationruntime.Runtime {
	if tx == nil || tx.relation == nil {
		return relationruntime.Default()
	}
	return tx.relation
}
