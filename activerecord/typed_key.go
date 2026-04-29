package activerecord

import (
	"context"
	"fmt"

	"github.com/arcgolabs/dbx/repository"
	"github.com/samber/mo"
)

// TypedKey binds an active-record store to one typed key column.
type TypedKey[E any, S repository.EntitySchema[E], T any] struct {
	store  *Store[E, S]
	access repository.TypedKey[E, S, T]
}

// By returns a typed key accessor for a store column.
func By[E any, S repository.EntitySchema[E], T any](store *Store[E, S], column repository.KeyColumn[T]) TypedKey[E, S, T] {
	var repo *repository.Base[E, S]
	if store != nil {
		repo = store.Repository()
	}
	return TypedKey[E, S, T]{
		store:  store,
		access: repository.By(repo, column),
	}
}

// Find loads a model identified by value.
func (k TypedKey[E, S, T]) Find(ctx context.Context, value T) (*Model[E, S], error) {
	entity, err := k.access.Get(ctx, value)
	if err != nil {
		return nil, fmt.Errorf("find entity by typed key: %w", err)
	}
	return k.store.newKeyedModel(&entity, k.store.keyOf(&entity)), nil
}

// FindOption loads a model identified by value as an option.
func (k TypedKey[E, S, T]) FindOption(ctx context.Context, value T) (mo.Option[*Model[E, S]], error) {
	entity, err := k.access.GetOption(ctx, value)
	if err != nil {
		return mo.None[*Model[E, S]](), fmt.Errorf("find entity by typed key: %w", err)
	}
	return k.store.wrapOption(entity), nil
}

// Exists reports whether value matches at least one row.
func (k TypedKey[E, S, T]) Exists(ctx context.Context, value T) (bool, error) {
	exists, err := k.access.Exists(ctx, value)
	if err != nil {
		return false, fmt.Errorf("check entity by typed key: %w", err)
	}
	return exists, nil
}
