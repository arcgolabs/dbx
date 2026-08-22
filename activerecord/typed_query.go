package activerecord

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// ListResult executes a typed querydsl SELECT using the default struct mapper for R.
func (s *Store[E, S]) ListResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (*collectionx.List[R], error) {
	repo, err := s.requireRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListResult(ctx, query)
}

// ListResultWithMapper executes a typed querydsl SELECT through store using mapper.
func (s *Store[E, S]) ListResultWithMapper[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
	mapper mapperx.RowsScanner[R],
) (*collectionx.List[R], error) {
	repo, err := s.requireRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListResultWithMapper(ctx, query, mapper)
}

// GetResult executes a typed querydsl SELECT through store and expects exactly one row.
func (s *Store[E, S]) GetResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (R, error) {
	repo, err := s.requireRepository()
	if err != nil {
		var zero R
		return zero, err
	}
	return repo.GetResult(ctx, query)
}

// FindResult executes a typed querydsl SELECT through store and returns zero or one row.
func (s *Store[E, S]) FindResult[R any](
	ctx context.Context,
	query querydsl.SelectResult[R],
) (mo.Option[R], error) {
	repo, err := s.requireRepository()
	if err != nil {
		return mo.None[R](), err
	}
	return repo.FindResult(ctx, query)
}

// ScalarResult executes a typed scalar querydsl SELECT through store and expects exactly one value.
func (s *Store[E, S]) ScalarResult[T any](
	ctx context.Context,
	query querydsl.SelectResult[T],
) (T, error) {
	repo, err := s.requireRepository()
	if err != nil {
		var zero T
		return zero, err
	}
	return repo.ScalarResult(ctx, query)
}

// ScalarResultOption executes a typed scalar querydsl SELECT through store and returns zero or one value.
func (s *Store[E, S]) ScalarResultOption[T any](
	ctx context.Context,
	query querydsl.SelectResult[T],
) (mo.Option[T], error) {
	repo, err := s.requireRepository()
	if err != nil {
		return mo.None[T](), err
	}
	return repo.ScalarResultOption(ctx, query)
}
