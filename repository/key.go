package repository

import (
	"context"
	"database/sql"

	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/querydsl"
)

// Key identifies a row by one or more column/value pairs.
type Key map[string]any

// GetByID returns the entity identified by the repository primary key.
func (r *Base[E, S]) GetByID(ctx context.Context, id any) (E, error) {
	pk := r.primaryColumnName()
	query := r.defaultSelect().Where(columnx.Named[any](r.schema, pk).Eq(id))
	return r.First(ctx, query)
}

// GetByKey returns the entity identified by the provided key columns.
func (r *Base[E, S]) GetByKey(ctx context.Context, key Key) (E, error) {
	if len(key) == 0 {
		var zero E
		return zero, &ValidationError{Message: "key is empty"}
	}
	return r.First(ctx, r.defaultSelect().Where(keyPredicate(r.schema, key)))
}

// UpdateByKey updates rows matched by the provided key.
func (r *Base[E, S]) UpdateByKey(ctx context.Context, key Key, assignments ...querydsl.Assignment) (sql.Result, error) {
	if len(key) == 0 {
		return nil, &ValidationError{Message: "key is empty"}
	}
	if len(assignments) == 0 {
		return nil, ErrNilMutation
	}
	result, err := r.Update(ctx, querydsl.Update(r.schema).Set(assignments...).Where(keyPredicate(r.schema, key)))
	if err != nil {
		return nil, err
	}
	if r.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}

// DeleteByKey deletes rows matched by the provided key.
func (r *Base[E, S]) DeleteByKey(ctx context.Context, key Key) (sql.Result, error) {
	if len(key) == 0 {
		return nil, &ValidationError{Message: "key is empty"}
	}
	result, err := r.Delete(ctx, querydsl.DeleteFrom(r.schema).Where(keyPredicate(r.schema, key)))
	if err != nil {
		return nil, err
	}
	if r.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}
