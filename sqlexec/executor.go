//revive:disable:file-length-limit Executor helpers are kept together to preserve the SQL execution API surface.

package sqlexec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
	"github.com/samber/oops"
	scanlib "github.com/stephenafamo/scan"
)

var (
	ErrNilSession  = errors.New("dbx/sqlexec: session is nil")
	ErrNilMapper   = errors.New("dbx/sqlexec: mapper is nil")
	ErrTooManyRows = errors.New("dbx/sqlexec: query returned more than one row")
)

type Session interface {
	QueryBoundContext(ctx context.Context, bound sqlstmt.Bound) (*sql.Rows, error)
	ExecBoundContext(ctx context.Context, bound sqlstmt.Bound) (sql.Result, error)
}

type RowsScanner[E any] interface {
	ScanRows(rows *sql.Rows) (collectionx.List[E], error)
}

type CapacityHintScanner[E any] interface {
	ScanRowsWithCapacity(rows *sql.Rows, capacityHint int) (collectionx.List[E], error)
}

type Executor struct {
	session Session
}

func New(session Session) *Executor {
	return &Executor{session: session}
}

func (x *Executor) Bind(statement sqlstmt.Source, params any) (sqlstmt.Bound, error) {
	if statement == nil {
		return sqlstmt.Bound{}, oops.In("dbx/sqlexec").
			With("op", "bind").
			Wrapf(sqlstmt.ErrNilStatement, "validate sql statement")
	}

	bound, err := statement.Bind(params)
	if err != nil {
		return sqlstmt.Bound{}, wrapError("bind sql statement", err)
	}
	if bound.Name == "" {
		bound.Name = statement.StatementName()
	}
	if bound.Args.Len() > 0 {
		bound.Args = bound.Args.Clone()
	}
	return bound, nil
}

func (x *Executor) Exec(ctx context.Context, statement sqlstmt.Source, params any) (sql.Result, error) {
	session, err := x.sessionOrErr()
	if err != nil {
		return nil, err
	}
	bound, err := x.Bind(statement, params)
	if err != nil {
		return nil, err
	}
	result, execErr := session.ExecBoundContext(ctx, bound)
	return result, wrapError("execute sql statement", execErr)
}

func (x *Executor) Query(ctx context.Context, statement sqlstmt.Source, params any) (*sql.Rows, error) {
	session, err := x.sessionOrErr()
	if err != nil {
		return nil, err
	}
	bound, err := x.Bind(statement, params)
	if err != nil {
		return nil, err
	}
	return queryBound(ctx, session, bound)
}

func (x *Executor) QueryBound(ctx context.Context, bound sqlstmt.Bound) (*sql.Rows, error) {
	session, err := x.sessionOrErr()
	if err != nil {
		return nil, err
	}
	return queryBound(ctx, session, bound)
}

func (x *Executor) sessionOrErr() (Session, error) {
	if x == nil || x.session == nil {
		return nil, oops.In("dbx/sqlexec").
			With("op", "session").
			Wrapf(ErrNilSession, "validate executor session")
	}
	return x.session, nil
}

func queryBound(ctx context.Context, session Session, bound sqlstmt.Bound) (*sql.Rows, error) {
	rows, queryErr := session.QueryBoundContext(ctx, bound)
	return rows, wrapError("query sql statement", queryErr)
}

func List[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (collectionx.List[E], error) {
	if mapper == nil {
		return nil, oops.In("dbx/sqlexec").
			With("op", "list", "statement", sqlstmt.Name(statement)).
			Wrapf(ErrNilMapper, "validate mapper")
	}

	exec, err := executor(session)
	if err != nil {
		return nil, err
	}
	rows, bound, err := queryStatementRows(ctx, exec, statement, params)
	if err != nil {
		return nil, err
	}
	if withCap, ok := capacityHintScannerFor(mapper, bound.CapacityHint); ok {
		return scanRowsWithCapacity(rows, bound, withCap)
	}
	return scanRows(rows, mapper)
}

func QueryList[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (collectionx.List[E], error) {
	return List(ctx, session, statement, params, mapper)
}

func Get[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (E, error) {
	items, err := List(ctx, session, statement, params, mapper)
	if err != nil {
		var zero E
		return zero, err
	}
	switch items.Len() {
	case 0:
		var zero E
		return zero, oops.In("dbx/sqlexec").
			With("op", "get", "statement", sqlstmt.Name(statement)).
			Wrapf(sql.ErrNoRows, "sql get returned no rows")
	case 1:
		item, _ := items.GetFirst()
		return item, nil
	default:
		var zero E
		return zero, oops.In("dbx/sqlexec").
			With("op", "get", "statement", sqlstmt.Name(statement)).
			Wrapf(ErrTooManyRows, "sql get returned too many rows")
	}
}

func Find[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (mo.Option[E], error) {
	items, err := List(ctx, session, statement, params, mapper)
	if err != nil {
		return mo.None[E](), err
	}
	switch items.Len() {
	case 0:
		return mo.None[E](), nil
	case 1:
		item, _ := items.GetFirst()
		return mo.Some(item), nil
	default:
		return mo.None[E](), oops.In("dbx/sqlexec").
			With("op", "find", "statement", sqlstmt.Name(statement)).
			Wrapf(ErrTooManyRows, "sql find returned too many rows")
	}
}

func Scalar[T any](ctx context.Context, session Session, statement sqlstmt.Source, params any) (T, error) {
	value, found, err := scalar[T](ctx, session, statement, params)
	if err != nil {
		var zero T
		return zero, err
	}
	if !found {
		var zero T
		return zero, oops.In("dbx/sqlexec").
			With("op", "scalar", "statement", sqlstmt.Name(statement)).
			Wrapf(sql.ErrNoRows, "sql scalar returned no rows")
	}
	return value, nil
}

func ScalarOption[T any](ctx context.Context, session Session, statement sqlstmt.Source, params any) (mo.Option[T], error) {
	value, found, err := scalar[T](ctx, session, statement, params)
	if err != nil {
		return mo.None[T](), err
	}
	if !found {
		return mo.None[T](), nil
	}
	return mo.Some(value), nil
}

func executor(session Session) (*Executor, error) {
	if session == nil {
		return nil, oops.In("dbx/sqlexec").
			With("op", "session").
			Wrapf(ErrNilSession, "validate session")
	}
	return New(session), nil
}

func queryStatementRows(ctx context.Context, executor *Executor, statement sqlstmt.Source, params any) (*sql.Rows, sqlstmt.Bound, error) {
	if executor == nil {
		return nil, sqlstmt.Bound{}, oops.In("dbx/sqlexec").
			With("op", "query_rows", "statement", sqlstmt.Name(statement)).
			Wrapf(ErrNilSession, "validate sql executor")
	}
	bound, err := executor.Bind(statement, params)
	if err != nil {
		return nil, sqlstmt.Bound{}, oops.In("dbx/sqlexec").
			With("op", "query_rows", "statement", sqlstmt.Name(statement)).
			Wrapf(err, "bind statement rows")
	}
	rows, err := executor.QueryBound(ctx, bound)
	if err != nil {
		return nil, sqlstmt.Bound{}, oops.In("dbx/sqlexec").
			With("op", "query_rows", "statement", sqlstmt.Name(statement)).
			Wrapf(err, "query statement rows")
	}
	return rows, bound, nil
}

func scanRows[E any](rows *sql.Rows, mapper RowsScanner[E]) (collectionx.List[E], error) {
	items, scanErr := mapper.ScanRows(rows)
	scanErr = errors.Join(wrapError("scan statement rows", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return items, nil
}

func scanRowsWithCapacity[E any](rows *sql.Rows, bound sqlstmt.Bound, mapper CapacityHintScanner[E]) (collectionx.List[E], error) {
	items, scanErr := mapper.ScanRowsWithCapacity(rows, bound.CapacityHint)
	scanErr = errors.Join(wrapError("scan statement rows with capacity", scanErr), rowsIterError(rows))
	closeErr := closeRows(rows)
	if scanErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return items, nil
}

func scalar[T any](ctx context.Context, session Session, statement sqlstmt.Source, params any) (T, bool, error) {
	exec, err := executor(session)
	if err != nil {
		var zero T
		return zero, false, err
	}
	rows, _, err := queryStatementRows(ctx, exec, statement, params)
	if err != nil {
		var zero T
		return zero, false, err
	}

	value, err := scanlib.OneFromRows[T](ctx, scanlib.SingleColumnMapper[T], rows)
	if err != nil {
		closeErr := closeRows(rows)
		var zero T
		if errors.Is(err, sql.ErrNoRows) {
			return zero, false, closeErr
		}
		return zero, false, errors.Join(wrapError("scan scalar row", err), closeErr)
	}
	if rows.Next() {
		closeErr := closeRows(rows)
		var zero T
		return zero, false, errors.Join(
			oops.In("dbx/sqlexec").
				With("op", "scalar", "statement", sqlstmt.Name(statement)).
				Wrapf(ErrTooManyRows, "sql scalar returned too many rows"),
			closeErr,
		)
	}
	if err := rowsIterError(rows); err != nil {
		closeErr := closeRows(rows)
		var zero T
		return zero, false, errors.Join(err, closeErr)
	}
	closeErr := closeRows(rows)
	if closeErr != nil {
		var zero T
		return zero, false, closeErr
	}
	return value, true, nil
}

func capacityHintScannerFor[E any](mapper RowsScanner[E], capacityHint int) (CapacityHintScanner[E], bool) {
	if capacityHint <= 0 {
		return nil, false
	}
	withCap, ok := any(mapper).(CapacityHintScanner[E])
	return withCap, ok
}

func closeRows(rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	return wrapError("close rows", rows.Close())
}

func rowsIterError(rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	return wrapError("read rows", rows.Err())
}

func wrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}
