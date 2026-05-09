package repository

import (
	"context"
	"errors"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
)

// Cursor returns a streaming cursor for entities matched by query.
func (r *Base[E, S]) Cursor(ctx context.Context, query *querydsl.SelectQuery) (dbx.Cursor[E], error) {
	return r.cursor(ctx, query, true)
}

func (r *Base[E, S]) cursor(ctx context.Context, query *querydsl.SelectQuery, includeDefaults bool) (dbx.Cursor[E], error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	cursorQuery := cloneOrDefaultWithDefaults(r, query, includeDefaults)
	return dbx.QueryCursor[E](ctx, r.session, cursorQuery, r.mapper)
}

// Each returns an iterator over entities matched by query.
func (r *Base[E, S]) Each(ctx context.Context, query *querydsl.SelectQuery) func(func(E, error) bool) {
	return func(yield func(E, error) bool) {
		cursor, err := r.Cursor(ctx, query)
		if err != nil {
			var zero E
			yield(zero, err)
			return
		}
		defer yieldRepositoryCursorCloseError(cursor, yield)
		for cursor.Next() {
			item, itemErr := cursor.Get()
			if !yield(item, itemErr) || itemErr != nil {
				return
			}
		}
		if err := cursor.Err(); err != nil {
			var zero E
			yield(zero, err)
		}
	}
}

// Batch reads entities matched by query in batches and invokes handle for each batch.
func (r *Base[E, S]) Batch(
	ctx context.Context,
	query *querydsl.SelectQuery,
	size int,
	handle func(*collectionx.List[E]) error,
) error {
	return r.batch(ctx, query, true, size, handle)
}

func (r *Base[E, S]) batch(
	ctx context.Context,
	query *querydsl.SelectQuery,
	includeDefaults bool,
	size int,
	handle func(*collectionx.List[E]) error,
	includes ...Include[E],
) (resultErr error) {
	if err := validateBatchRequest(size, handle); err != nil {
		return err
	}
	cursor, err := r.cursor(ctx, query, includeDefaults)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cursor.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close repository cursor: %w", closeErr))
		}
	}()
	return drainRepositoryBatches(ctx, cursor, size, handle, includes...)
}

func validateBatchRequest[E any](size int, handle func(*collectionx.List[E]) error) error {
	if size <= 0 {
		return &ValidationError{Message: "batch size must be positive"}
	}
	if handle == nil {
		return &ValidationError{Message: "batch handler is nil"}
	}
	return nil
}

func drainRepositoryBatches[E any](
	ctx context.Context,
	cursor dbx.Cursor[E],
	size int,
	handle func(*collectionx.List[E]) error,
	includes ...Include[E],
) error {
	batch := collectionx.NewListWithCapacity[E](size)
	for cursor.Next() {
		item, itemErr := cursor.Get()
		if itemErr != nil {
			return fmt.Errorf("get repository cursor item: %w", itemErr)
		}
		batch.Add(item)
		if batch.Len() >= size {
			if err := flushRepositoryBatch(ctx, batch, handle, includes...); err != nil {
				return err
			}
			batch = collectionx.NewListWithCapacity[E](size)
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("repository cursor iteration: %w", err)
	}
	return flushRepositoryBatch(ctx, batch, handle, includes...)
}

func flushRepositoryBatch[E any](
	ctx context.Context,
	batch *collectionx.List[E],
	handle func(*collectionx.List[E]) error,
	includes ...Include[E],
) error {
	if batch.IsEmpty() {
		return nil
	}
	if err := LoadIncludes(ctx, batch, includes...); err != nil {
		return err
	}
	if err := handle(batch); err != nil {
		return fmt.Errorf("handle repository batch: %w", err)
	}
	return nil
}

func yieldRepositoryCursorCloseError[E any](cursor dbx.Cursor[E], yield func(E, error) bool) {
	if closeErr := cursor.Close(); closeErr != nil {
		var zero E
		yield(zero, closeErr)
	}
}
