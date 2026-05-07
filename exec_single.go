package dbx

import (
	"context"
	"database/sql"
	"errors"

	collectionx "github.com/arcgolabs/collectionx/list"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
	"github.com/samber/oops"
)

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

// QueryOneBound executes a pre-built sqlstmt.Bound and maps exactly one row.
func QueryOneBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (E, error) {
	items, err := queryRowsLimitBound[E](ctx, session, bound, mapper, 2, "query_one_bound")
	if err != nil {
		var zero E
		return zero, err
	}
	return oneFromList[E](items, "query_one_bound")
}

// QueryOptionBound executes a pre-built sqlstmt.Bound and maps zero or one row.
func QueryOptionBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (mo.Option[E], error) {
	items, err := queryRowsLimitBound[E](ctx, session, bound, mapper, 2, "query_option_bound")
	if err != nil {
		return mo.None[E](), err
	}
	return optionFromList[E](items, "query_option_bound")
}

func queryRowsLimitBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E], limit int, op string) (*collectionx.List[E], error) {
	limitScanner, ok := any(mapper).(mapperx.LimitScanner[E])
	if !ok {
		return QueryAllBound[E](ctx, session, bound, mapper)
	}
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", op, "statement", bound.Name).
			Wrapf(ErrNilMapper, "validate mapper")
	}
	if session == nil {
		return nil, oops.In("dbx").
			With("op", op, "statement", bound.Name).
			Wrapf(ErrNilDB, "validate session")
	}
	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		logRuntimeNode(session, op+".query_error", "statement", bound.Name, "error", err)
		return nil, wrapDBError("query bound rows", err)
	}

	logRuntimeNode(session, op+".scan_limit", "limit", limit)
	items, scanErr := limitScanner.ScanRowsLimit(ctx, rows, limit)
	scanErr = errors.Join(wrapDBError("scan rows limit", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		scanErr = errors.Join(scanErr, closeErr)
		logRuntimeNode(session, op+".scan_limit_error", "error", scanErr)
		return nil, scanErr
	}
	if closeErr != nil {
		logRuntimeNode(session, op+".scan_limit_error", "error", closeErr)
		return nil, closeErr
	}
	logRuntimeNode(session, op+".scan_limit_done", "items", items.Len())
	return items, nil
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
