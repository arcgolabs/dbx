package dbx

import (
	"context"
	"database/sql"
	"errors"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/samber/oops"
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *Row
}

type Scanner[T any] func(rows *sql.Rows) (T, error)

type Session interface {
	Executor
	Dialect() dialect.Dialect
	QueryBoundContext(ctx context.Context, bound sqlstmt.Bound) (*sql.Rows, error)
	ExecBoundContext(ctx context.Context, bound sqlstmt.Bound) (sql.Result, error)
	// SQL returns an executor for templated SQL. DB and Tx implement this for unified execution entry.
	SQL() *sqlexec.Executor
}

// Build compiles a querydsl.Builder into sqlstmt.Bound using the session's dialect.
// For "build once, execute many" reuse: call Build once, then pass the result to
// ExecBound, QueryAllBound, QueryCursorBound, or QueryEachBound in a loop.
func Build(session Session, query querydsl.Builder) (sqlstmt.Bound, error) {
	if session == nil {
		return sqlstmt.Bound{}, oops.In("dbx").
			With("op", "build_query").
			Wrapf(ErrNilDB, "validate session")
	}
	if session.Dialect() == nil {
		return sqlstmt.Bound{}, oops.In("dbx").
			With("op", "build_query").
			Wrapf(ErrNilDialect, "validate dialect")
	}
	if query == nil {
		logRuntimeNode(session, "build.error", "error", ErrNilQuery)
		return sqlstmt.Bound{}, oops.In("dbx").
			With("op", "build_query").
			Wrapf(ErrNilQuery, "validate query")
	}
	logRuntimeNode(session, "build.start")
	bound, err := query.Build(session.Dialect())
	if err != nil {
		logRuntimeNode(session, "build.error", "error", err)
		return sqlstmt.Bound{}, wrapDBError("build query", err)
	}
	logRuntimeNode(session, "build.done", "sql_empty", bound.SQL == "", "args_count", bound.Args.Len())
	return bound, nil
}

func Exec(ctx context.Context, session Session, query querydsl.Builder) (sql.Result, error) {
	bound, err := Build(session, query)
	if err != nil {
		return nil, err
	}
	logRuntimeNode(session, "exec.bound_ready", "statement", bound.Name, "args_count", bound.Args.Len())
	return ExecBound(ctx, session, bound)
}

// ExecBound executes a pre-built sqlstmt.Bound. Use with Build for reuse when
// executing the same query multiple times (e.g. in a loop).
func ExecBound(ctx context.Context, session Session, bound sqlstmt.Bound) (sql.Result, error) {
	if session == nil {
		return nil, oops.In("dbx").
			With("op", "exec_bound", "statement", bound.Name).
			Wrapf(ErrNilDB, "validate session")
	}
	logRuntimeNode(session, "exec_bound.start", "statement", bound.Name, "args_count", bound.Args.Len())
	result, err := session.ExecBoundContext(ctx, bound)
	return result, wrapDBError("execute bound query", err)
}

func QueryAll[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (collectionx.List[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "query_all").
			Wrapf(ErrNilMapper, "validate mapper")
	}
	bound, err := Build(session, query)
	if err != nil {
		return nil, err
	}
	return QueryAllBound[E](ctx, session, bound, mapper)
}

// QueryAllList builds a query and maps all rows into a collectionx.List.
func QueryAllList[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (collectionx.List[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "query_all_list").
			Wrapf(ErrNilMapper, "validate mapper")
	}
	bound, err := Build(session, query)
	if err != nil {
		return nil, err
	}
	return QueryAllBoundList[E](ctx, session, bound, mapper)
}

// QueryAllBound executes a pre-built sqlstmt.Bound and maps all rows. Use with Build
// for reuse when executing the same query multiple times.
// When bound.CapacityHint > 0 and mapper implements CapacityHintScanner, uses
// pre-allocated slice to reduce append growth.
func QueryAllBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (collectionx.List[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "query_all_bound", "statement", bound.Name).
			Wrapf(ErrNilMapper, "validate mapper")
	}
	if session == nil {
		return nil, oops.In("dbx").
			With("op", "query_all_bound", "statement", bound.Name).
			Wrapf(ErrNilDB, "validate session")
	}
	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		logRuntimeNode(session, "query_all_bound.query_error", "statement", bound.Name, "error", err)
		return nil, wrapDBError("query bound rows", err)
	}
	if withCap, ok := capacityHintScannerFor[E](mapper, bound.CapacityHint); ok {
		return scanAllBoundWithCapacity[E](session, rows, bound, withCap)
	}
	logRuntimeNode(session, "query_all_bound.scan")
	items, scanErr := mapper.ScanRows(rows)
	scanErr = errors.Join(wrapDBError("scan rows", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		scanErr = errors.Join(scanErr, closeErr)
		logRuntimeNode(session, "query_all_bound.scan_error", "error", scanErr)
		return nil, scanErr
	}
	if closeErr != nil {
		logRuntimeNode(session, "query_all_bound.scan_error", "error", closeErr)
		return nil, closeErr
	}
	logRuntimeNode(session, "query_all_bound.scan_done", "items", items.Len())
	return items, nil
}

// QueryAllBoundList executes a pre-built sqlstmt.Bound and maps all rows into a collectionx.List.
func QueryAllBoundList[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (collectionx.List[E], error) {
	return QueryAllBound[E](ctx, session, bound, mapper)
}

func capacityHintScannerFor[E any](mapper mapperx.RowsScanner[E], capacityHint int) (mapperx.CapacityHintScanner[E], bool) {
	if capacityHint <= 0 {
		return nil, false
	}
	withCap, ok := any(mapper).(mapperx.CapacityHintScanner[E])
	return withCap, ok
}

func scanAllBoundWithCapacity[E any](session Session, rows *sql.Rows, bound sqlstmt.Bound, mapper mapperx.CapacityHintScanner[E]) (collectionx.List[E], error) {
	logRuntimeNode(session, "query_all_bound.scan_with_capacity", "capacity_hint", bound.CapacityHint)
	items, scanErr := mapper.ScanRowsWithCapacity(rows, bound.CapacityHint)
	scanErr = errors.Join(wrapDBError("scan rows with capacity", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		scanErr = errors.Join(scanErr, closeErr)
		logRuntimeNode(session, "query_all_bound.scan_error", "error", scanErr)
		return nil, scanErr
	}
	if closeErr != nil {
		logRuntimeNode(session, "query_all_bound.scan_error", "error", closeErr)
		return nil, closeErr
	}
	logRuntimeNode(session, "query_all_bound.scan_done", "items", items.Len())
	return items, nil
}
