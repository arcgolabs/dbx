package repository

import (
	"context"
	"database/sql"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// KeyColumn is the typed column behavior needed for key lookups.
type KeyColumn[T any] interface {
	Eq(T) querydsl.Predicate
}

// TypedKey binds a repository to one typed key column.
type TypedKey[E any, S EntitySchema[E], T any] struct {
	repo   *Base[E, S]
	column KeyColumn[T]
}

// By returns a typed key accessor for a repository column.
func By[E any, S EntitySchema[E], T any](repo *Base[E, S], column KeyColumn[T]) TypedKey[E, S, T] {
	return TypedKey[E, S, T]{repo: repo, column: column}
}

// Get returns the entity identified by value.
func (k TypedKey[E, S, T]) Get(ctx context.Context, value T) (E, error) {
	var zero E
	if k.repo == nil {
		return zero, dbx.ErrNilDB
	}
	return k.repo.First(ctx, k.selectQuery(value))
}

// GetOption returns the entity identified by value as an option.
func (k TypedKey[E, S, T]) GetOption(ctx context.Context, value T) (mo.Option[E], error) {
	return optionFromResult(k.Get(ctx, value))
}

// Exists reports whether value matches at least one row.
func (k TypedKey[E, S, T]) Exists(ctx context.Context, value T) (bool, error) {
	if k.repo == nil {
		return false, dbx.ErrNilDB
	}
	return k.repo.Exists(ctx, k.selectQuery(value))
}

// Update updates the row identified by value.
func (k TypedKey[E, S, T]) Update(ctx context.Context, value T, assignments ...querydsl.Assignment) (sql.Result, error) {
	if k.repo == nil {
		return nil, dbx.ErrNilDB
	}
	if len(assignments) == 0 {
		return nil, ErrNilMutation
	}
	result, err := k.repo.Update(ctx, querydsl.Update(k.repo.schema).Set(assignments...).Where(k.column.Eq(value)))
	if err != nil {
		return nil, err
	}
	if k.repo.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}

// Delete deletes the row identified by value.
func (k TypedKey[E, S, T]) Delete(ctx context.Context, value T) (sql.Result, error) {
	if k.repo == nil {
		return nil, dbx.ErrNilDB
	}
	result, err := k.repo.Delete(ctx, querydsl.DeleteFrom(k.repo.schema).Where(k.column.Eq(value)))
	if err != nil {
		return nil, err
	}
	if k.repo.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}

func (k TypedKey[E, S, T]) selectQuery(value T) *querydsl.SelectQuery {
	return k.repo.defaultSelect().Where(k.column.Eq(value))
}
