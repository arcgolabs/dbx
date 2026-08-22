package dbx

import (
	"context"
	"database/sql"
	"errors"

	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
	"github.com/samber/oops"
)

// QueryScalar builds a typed scalar querydsl SELECT and scans exactly one single-column row.
func QueryScalar[T any](ctx context.Context, session Session, query querydsl.SelectResult[T]) (T, error) {
	bound, err := Build(session, query)
	if err != nil {
		var zero T
		return zero, err
	}
	return QueryScalarBound[T](ctx, session, bound)
}

// QueryScalarOption builds a typed scalar querydsl SELECT and scans zero or one single-column row.
func QueryScalarOption[T any](ctx context.Context, session Session, query querydsl.SelectResult[T]) (mo.Option[T], error) {
	bound, err := Build(session, query)
	if err != nil {
		return mo.None[T](), err
	}
	return QueryScalarOptionBound[T](ctx, session, bound)
}

// QueryScalarBound scans exactly one single-column row from a pre-built bound query.
func QueryScalarBound[T any](ctx context.Context, session Session, bound sqlstmt.Bound) (T, error) {
	value, found, err := scalarBound[T](ctx, session, bound, "query_scalar_bound")
	if err != nil {
		var zero T
		return zero, err
	}
	if !found {
		var zero T
		return zero, oops.In("dbx").
			With("op", "query_scalar_bound", "statement", bound.Name).
			Wrapf(sql.ErrNoRows, "query scalar returned no rows")
	}
	return value, nil
}

// QueryScalarOptionBound scans zero or one single-column row from a pre-built bound query.
func QueryScalarOptionBound[T any](ctx context.Context, session Session, bound sqlstmt.Bound) (mo.Option[T], error) {
	value, found, err := scalarBound[T](ctx, session, bound, "query_scalar_option_bound")
	if err != nil {
		return mo.None[T](), err
	}
	if !found {
		return mo.None[T](), nil
	}
	return mo.Some(value), nil
}

func scalarBound[T any](ctx context.Context, session Session, bound sqlstmt.Bound, op string) (T, bool, error) {
	if session == nil {
		var zero T
		return zero, false, oops.In("dbx").
			With("op", op, "statement", bound.Name).
			Wrapf(ErrNilDB, "validate session")
	}

	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		var zero T
		return zero, false, wrapDBError("query scalar bound", err)
	}

	if !rows.Next() {
		iterErr := rowsIterError(rows)
		closeErr := closeRows(rows)
		var zero T
		if iterErr != nil {
			return zero, false, errors.Join(iterErr, closeErr)
		}
		return zero, false, closeErr
	}
	var value T
	if err := rows.Scan(&value); err != nil {
		closeErr := closeRows(rows)
		var zero T
		return zero, false, errors.Join(wrapDBError("scan scalar row", err), closeErr)
	}
	if rows.Next() {
		closeErr := closeRows(rows)
		var zero T
		return zero, false, errors.Join(
			oops.In("dbx").
				With("op", op, "statement", bound.Name).
				Wrapf(ErrTooManyRows, "query scalar returned too many rows"),
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
