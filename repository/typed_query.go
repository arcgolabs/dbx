package repository

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// ListResult executes a typed querydsl SELECT using the default struct mapper for R.
func (r *Base[E, S]) ListResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (*collectionx.List[R], error) {
	session, err := r.sessionOrError()
	if err != nil {
		return nil, err
	}
	return dbx.QueryTyped[R](ctx, session, query)
}

// ListResultWithMapper executes a typed querydsl SELECT using mapper.
func (r *Base[E, S]) ListResultWithMapper[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
	mapper mapperx.RowsScanner[R],
) (*collectionx.List[R], error) {
	session, err := r.sessionOrError()
	if err != nil {
		return nil, err
	}
	return dbx.QueryAllTyped[R](ctx, session, query, mapper)
}

// GetResult executes a typed querydsl SELECT and expects exactly one row.
func (r *Base[E, S]) GetResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (R, error) {
	session, err := r.sessionOrError()
	if err != nil {
		var zero R
		return zero, err
	}
	return dbx.GetTyped[R](ctx, session, query)
}

// FindResult executes a typed querydsl SELECT and returns zero or one row.
func (r *Base[E, S]) FindResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (mo.Option[R], error) {
	session, err := r.sessionOrError()
	if err != nil {
		return mo.None[R](), err
	}
	return dbx.FindTyped[R](ctx, session, query)
}

// ScalarResult executes a typed scalar querydsl SELECT and expects exactly one value.
func (r *Base[E, S]) ScalarResult[T any](
	ctx context.Context,
	query querydsl.SelectResult[T],
) (T, error) {
	session, err := r.sessionOrError()
	if err != nil {
		var zero T
		return zero, err
	}
	return dbx.QueryScalar[T](ctx, session, query)
}

// ScalarResultOption executes a typed scalar querydsl SELECT and returns zero or one value.
func (r *Base[E, S]) ScalarResultOption[T any](
	ctx context.Context,
	query querydsl.SelectResult[T],
) (mo.Option[T], error) {
	session, err := r.sessionOrError()
	if err != nil {
		return mo.None[T](), err
	}
	return dbx.QueryScalarOption[T](ctx, session, query)
}

func (r *Base[E, S]) sessionOrError() (dbx.Session, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	return r.session, nil
}
