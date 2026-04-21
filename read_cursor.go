package dbx

import (
	"context"
	"database/sql"
	"errors"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/samber/oops"
)

type Cursor[T any] interface {
	Close() error
	Next() bool
	Get() (T, error)
	Err() error
}

type sliceCursor[E any] struct {
	items collectionx.List[E]
	index int
}

func newSliceCursor[E any](items collectionx.List[E]) Cursor[E] {
	return &sliceCursor[E]{items: items, index: -1}
}

func (c *sliceCursor[E]) Close() error {
	return nil
}

func (c *sliceCursor[E]) Next() bool {
	if c.index+1 >= c.items.Len() {
		return false
	}
	c.index++
	return true
}

func (c *sliceCursor[E]) Get() (E, error) {
	if c.index < 0 || c.index >= c.items.Len() {
		var zero E
		return zero, oops.In("dbx").
			With("op", "cursor_get", "index", c.index, "item_count", c.items.Len()).
			Wrapf(sql.ErrNoRows, "cursor is not positioned on a row")
	}
	item, _ := c.items.Get(c.index)
	return item, nil
}

func (c *sliceCursor[E]) Err() error {
	return nil
}

func QueryCursor[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) (Cursor[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "query_cursor").
			Wrapf(ErrNilMapper, "validate mapper")
	}
	bound, err := Build(session, query)
	if err != nil {
		return nil, err
	}
	return QueryCursorBound[E](ctx, session, bound, mapper)
}

// QueryCursorBound executes a pre-built sqlstmt.Bound and returns a cursor. Use with Build
// for reuse when executing the same query multiple times.
func QueryCursorBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) (Cursor[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "query_cursor_bound", "statement", bound.Name).
			Wrapf(ErrNilMapper, "validate mapper")
	}
	if session == nil {
		return nil, oops.In("dbx").
			With("op", "query_cursor_bound", "statement", bound.Name).
			Wrapf(ErrNilDB, "validate session")
	}
	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		logRuntimeNode(session, "query_cursor_bound.query_error", "statement", bound.Name, "error", err)
		return nil, wrapDBError("query cursor rows", err)
	}

	if cursor, ok, err := structMapperCursor(ctx, rows, mapper); ok {
		logRuntimeNode(session, "query_cursor_bound.scan_cursor")
		if err != nil {
			err = errors.Join(wrapDBError("scan cursor rows", err), closeRows(rows))
			logRuntimeNode(session, "query_cursor_bound.scan_cursor_error", "error", err)
			return nil, err
		}
		logRuntimeNode(session, "query_cursor_bound.scan_cursor_done")
		return cursor, nil
	}

	logRuntimeNode(session, "query_cursor_bound.scan_slice")
	items, scanErr := mapper.ScanRows(rows)
	scanErr = errors.Join(wrapDBError("scan cursor rows into slice", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		scanErr = errors.Join(scanErr, closeErr)
		logRuntimeNode(session, "query_cursor_bound.scan_slice_error", "error", scanErr)
		return nil, scanErr
	}
	if closeErr != nil {
		logRuntimeNode(session, "query_cursor_bound.scan_slice_error", "error", closeErr)
		return nil, closeErr
	}
	logRuntimeNode(session, "query_cursor_bound.scan_slice_done", "items", items.Len())
	return newSliceCursor(items), nil
}

func QueryEach[E any](ctx context.Context, session Session, query querydsl.Builder, mapper mapperx.RowsScanner[E]) func(func(E, error) bool) {
	return iterateCursor(func() (Cursor[E], error) {
		return QueryCursor[E](ctx, session, query, mapper)
	})
}

func SQLCursor[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper mapperx.RowsScanner[E]) (Cursor[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx").
			With("op", "sql_cursor", "statement", sqlstmt.Name(statement)).
			Wrapf(ErrNilMapper, "validate mapper")
	}

	rows, err := sqlexec.New(session).Query(ctx, statement, params)
	if err != nil {
		return nil, wrapDBError("query sql cursor rows", err)
	}

	if cursor, ok, err := structMapperCursor(ctx, rows, mapper); ok {
		if err != nil {
			return nil, errors.Join(wrapDBError("scan sql cursor rows", err), closeRows(rows))
		}
		return cursor, nil
	}

	items, scanErr := mapper.ScanRows(rows)
	scanErr = errors.Join(wrapDBError("scan sql rows", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return newSliceCursor(items), nil
}

// QueryEachBound is the sqlstmt.Bound variant of QueryEach. Use with Build for reuse.
func QueryEachBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper mapperx.RowsScanner[E]) func(func(E, error) bool) {
	return iterateCursor(func() (Cursor[E], error) {
		return QueryCursorBound[E](ctx, session, bound, mapper)
	})
}

func SQLEach[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper mapperx.RowsScanner[E]) func(func(E, error) bool) {
	return iterateCursor(func() (Cursor[E], error) {
		return SQLCursor[E](ctx, session, statement, params, mapper)
	})
}

func structMapperCursor[E any](ctx context.Context, rows *sql.Rows, mapper mapperx.RowsScanner[E]) (Cursor[E], bool, error) {
	switch typed := any(mapper).(type) {
	case mapperx.StructMapper[E]:
		cursor, err := typed.ScanCursor(ctx, rows)
		return cursor, true, wrapDBError("open mapper scan cursor", err)
	case *mapperx.StructMapper[E]:
		if typed == nil {
			return nil, true, oops.In("dbx").
				With("op", "cursor_mapper").
				Wrapf(ErrNilMapper, "validate struct mapper")
		}
		cursor, err := typed.ScanCursor(ctx, rows)
		return cursor, true, wrapDBError("open mapper scan cursor", err)
	default:
		return nil, false, nil
	}
}

func iterateCursor[E any](open func() (Cursor[E], error)) func(func(E, error) bool) {
	return func(yield func(E, error) bool) {
		cursor, ok := openCursorOrYieldError(open, yield)
		if !ok {
			return
		}
		defer yieldCursorCloseError(cursor, yield)
		if !drainCursor(cursor, yield) {
			return
		}
		yieldCursorErr(cursor, yield)
	}
}

func openCursorOrYieldError[E any](open func() (Cursor[E], error), yield func(E, error) bool) (Cursor[E], bool) {
	cursor, err := open()
	if err == nil {
		return cursor, true
	}
	var zero E
	yield(zero, err)
	return nil, false
}

func drainCursor[E any](cursor Cursor[E], yield func(E, error) bool) bool {
	for cursor.Next() {
		if !yieldCursorItem(cursor, yield) {
			return false
		}
	}
	return true
}

func yieldCursorItem[E any](cursor Cursor[E], yield func(E, error) bool) bool {
	item, itemErr := cursor.Get()
	if !yield(item, itemErr) {
		return false
	}
	return itemErr == nil
}

func yieldCursorErr[E any](cursor Cursor[E], yield func(E, error) bool) {
	if err := cursor.Err(); err != nil {
		var zero E
		yield(zero, err)
	}
}

func yieldCursorCloseError[E any](cursor Cursor[E], yield func(E, error) bool) {
	if closeErr := closeCursor(cursor); closeErr != nil {
		var zero E
		yield(zero, closeErr)
	}
}
