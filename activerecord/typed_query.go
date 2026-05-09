package activerecord

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/repository"
	"github.com/samber/mo"
)

// ListResult executes a typed querydsl SELECT through store using the default struct mapper for R.
func ListResult[R any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[R],
) (*collectionx.List[R], error) {
	repo, err := store.requireRepository()
	if err != nil {
		return nil, err
	}
	return repository.ListResult[R](ctx, repo, query)
}

// ListResultWithMapper executes a typed querydsl SELECT through store using mapper.
func ListResultWithMapper[R any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[R],
	mapper mapperx.RowsScanner[R],
) (*collectionx.List[R], error) {
	repo, err := store.requireRepository()
	if err != nil {
		return nil, err
	}
	return repository.ListResultWithMapper[R](ctx, repo, query, mapper)
}

// GetResult executes a typed querydsl SELECT through store and expects exactly one row.
func GetResult[R any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[R],
) (R, error) {
	repo, err := store.requireRepository()
	if err != nil {
		var zero R
		return zero, err
	}
	return repository.GetResult[R](ctx, repo, query)
}

// FindResult executes a typed querydsl SELECT through store and returns zero or one row.
func FindResult[R any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[R],
) (mo.Option[R], error) {
	repo, err := store.requireRepository()
	if err != nil {
		return mo.None[R](), err
	}
	return repository.FindResult[R](ctx, repo, query)
}

// ScalarResult executes a typed scalar querydsl SELECT through store and expects exactly one value.
func ScalarResult[T any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[T],
) (T, error) {
	repo, err := store.requireRepository()
	if err != nil {
		var zero T
		return zero, err
	}
	return repository.ScalarResult[T](ctx, repo, query)
}

// ScalarResultOption executes a typed scalar querydsl SELECT through store and returns zero or one value.
func ScalarResultOption[T any, E any, S repository.EntitySchema[E]](
	ctx context.Context,
	store *Store[E, S],
	query querydsl.SelectResult[T],
) (mo.Option[T], error) {
	repo, err := store.requireRepository()
	if err != nil {
		return mo.None[T](), err
	}
	return repository.ScalarResultOption[T](ctx, repo, query)
}
