package dbx

import (
	"context"
	"database/sql"
	"errors"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/samber/mo"
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

func QueryAll[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (*collectionx.List[E], error) {
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

// QueryAllTyped builds a typed querydsl SELECT and maps all rows into E.
func QueryAllTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E], mapper mapperx.RowsScanner[E]) (*collectionx.List[E], error) {
	return QueryAll[E](ctx, session, query, mapper)
}

// QueryTyped builds a typed querydsl SELECT and maps all rows into E with the default struct mapper.
func QueryTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E]) (*collectionx.List[E], error) {
	return QueryAllTyped[E](ctx, session, query, mapperx.MustStructMapper[E]())
}

// QueryOne builds a querydsl SELECT and maps exactly one row into E.
func QueryOne[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (E, error) {
	if mapper == nil {
		var zero E
		return zero, oops.In("dbx").
			With("op", "query_one").
			Wrapf(ErrNilMapper, "validate mapper")
	}
	bound, err := Build(session, query)
	if err != nil {
		var zero E
		return zero, err
	}
	return QueryOneBound[E](ctx, session, bound, mapper)
}

// QueryOneTyped builds a typed querydsl SELECT and maps exactly one row into E.
func QueryOneTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E], mapper mapperx.RowsScanner[E]) (E, error) {
	return QueryOne[E](ctx, session, query, mapper)
}

// GetTyped builds a typed querydsl SELECT and maps exactly one row into E with the default struct mapper.
func GetTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E]) (E, error) {
	return QueryOneTyped[E](ctx, session, query, mapperx.MustStructMapper[E]())
}

// QueryOption builds a querydsl SELECT and maps zero or one row into E.
func QueryOption[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (mo.Option[E], error) {
	if mapper == nil {
		return mo.None[E](), oops.In("dbx").
			With("op", "query_option").
			Wrapf(ErrNilMapper, "validate mapper")
	}
	bound, err := Build(session, query)
	if err != nil {
		return mo.None[E](), err
	}
	return QueryOptionBound[E](ctx, session, bound, mapper)
}

// QueryOptionTyped builds a typed querydsl SELECT and maps zero or one row into E.
func QueryOptionTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E], mapper mapperx.RowsScanner[E]) (mo.Option[E], error) {
	return QueryOption[E](ctx, session, query, mapper)
}

// FindTyped builds a typed querydsl SELECT and maps zero or one row into E with the default struct mapper.
func FindTyped[E any](ctx context.Context, session Session, query querydsl.SelectResult[E]) (mo.Option[E], error) {
	return QueryOptionTyped[E](ctx, session, query, mapperx.MustStructMapper[E]())
}

// QueryAllList builds a query and maps all rows into a collectionx.List.
func QueryAllList[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (*collectionx.List[E], error) {
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
func QueryAllBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (*collectionx.List[E], error) {
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
func QueryAllBoundList[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (*collectionx.List[E], error) {
	return QueryAllBound[E](ctx, session, bound, mapper)
}

// QueryOneBound executes a pre-built sqlstmt.Bound and maps exactly one row.
func QueryOneBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (E, error) {
	items, err := QueryAllBound[E](ctx, session, bound, mapper)
	if err != nil {
		var zero E
		return zero, err
	}
	return oneFromList[E](items, "query_one_bound")
}

// QueryOptionBound executes a pre-built sqlstmt.Bound and maps zero or one row.
func QueryOptionBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (mo.Option[E], error) {
	items, err := QueryAllBound[E](ctx, session, bound, mapper)
	if err != nil {
		return mo.None[E](), err
	}
	return optionFromList[E](items, "query_option_bound")
}

func oneFromList[E any](items *collectionx.List[E], op string) (E, error) {
	if items == nil || items.Len() == 0 {
		var zero E
		return zero, oops.In("dbx").
			With("op", op).
			Wrapf(sql.ErrNoRows, "query returned no rows")
	}
	if items.Len() > 1 {
		var zero E
		return zero, oops.In("dbx").
			With("op", op).
			Wrapf(ErrTooManyRows, "query returned too many rows")
	}
	item, _ := items.GetFirst()
	return item, nil
}

func optionFromList[E any](items *collectionx.List[E], op string) (mo.Option[E], error) {
	if items == nil || items.Len() == 0 {
		return mo.None[E](), nil
	}
	if items.Len() > 1 {
		return mo.None[E](), oops.In("dbx").
			With("op", op).
			Wrapf(ErrTooManyRows, "query returned too many rows")
	}
	item, _ := items.GetFirst()
	return mo.Some(item), nil
}

func capacityHintScannerFor[E any](mapper mapperx.RowsScanner[E], capacityHint int) (mapperx.CapacityHintScanner[E], bool) {
	if capacityHint <= 0 {
		return nil, false
	}
	withCap, ok := any(mapper).(mapperx.CapacityHintScanner[E])
	return withCap, ok
}

func scanAllBoundWithCapacity[E any](session Session, rows *sql.Rows, bound sqlstmt.Bound, mapper mapperx.CapacityHintScanner[E]) (*collectionx.List[E], error) {
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
