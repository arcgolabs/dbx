package repository

import (
	"context"
	"database/sql"

	"github.com/arcgolabs/dbx/querydsl"
)

// Key identifies a row by one or more column/value pairs.
type Key map[string]any

// GetByKey returns the entity identified by the provided key columns.
func (r *Base[E, S]) GetByKey(ctx context.Context, key Key) (E, error) {
	if len(key) == 0 {
		var zero E
		return zero, &ValidationError{Message: "key is empty"}
	}
	return r.first(ctx, r.applySpecs(Where(keyPredicate(r.schema, key))), false)
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
	if r.keyNotFoundAsError && !hasAffectedRows(result) {
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
	if r.keyNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}
