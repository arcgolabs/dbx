package repository

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// ListResult executes a typed querydsl SELECT through repo using the default struct mapper for R.
func ListResult[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[R],
) (*collectionx.List[R], error) {
	session, err := repositorySession(repo)
	if err != nil {
		return nil, err
	}
	return dbx.QueryTyped[R](ctx, session, query)
}

// ListResultWithMapper executes a typed querydsl SELECT through repo using mapper.
func ListResultWithMapper[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[R],
	mapper mapperx.RowsScanner[R],
) (*collectionx.List[R], error) {
	session, err := repositorySession(repo)
	if err != nil {
		return nil, err
	}
	return dbx.QueryAllTyped[R](ctx, session, query, mapper)
}

// GetResult executes a typed querydsl SELECT through repo and expects exactly one row.
func GetResult[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[R],
) (R, error) {
	session, err := repositorySession(repo)
	if err != nil {
		var zero R
		return zero, err
	}
	return dbx.GetTyped[R](ctx, session, query)
}

// FindResult executes a typed querydsl SELECT through repo and returns zero or one row.
func FindResult[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[R],
) (mo.Option[R], error) {
	session, err := repositorySession(repo)
	if err != nil {
		return mo.None[R](), err
	}
	return dbx.FindTyped[R](ctx, session, query)
}

// ScalarResult executes a typed scalar querydsl SELECT through repo and expects exactly one value.
func ScalarResult[T any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[T],
) (T, error) {
	session, err := repositorySession(repo)
	if err != nil {
		var zero T
		return zero, err
	}
	return dbx.QueryScalar[T](ctx, session, query)
}

// ScalarResultOption executes a typed scalar querydsl SELECT through repo and returns zero or one value.
func ScalarResultOption[T any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query querydsl.SelectResult[T],
) (mo.Option[T], error) {
	session, err := repositorySession(repo)
	if err != nil {
		return mo.None[T](), err
	}
	return dbx.QueryScalarOption[T](ctx, session, query)
}

func repositorySession[E any, S EntitySchema[E]](repo *Base[E, S]) (dbx.Session, error) {
	if repo == nil || repo.session == nil {
		return nil, dbx.ErrNilDB
	}
	return repo.session, nil
}
