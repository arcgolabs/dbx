package repository

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationload"
)

// Relations constructs a relation loader from two repositories.
func Relations[E any, S EntitySchema[E], T any, TS EntitySchema[T]](source *Base[E, S], target *Base[T, TS]) (relationload.Loader[E, T], error) {
	if source == nil || source.session == nil || target == nil {
		return relationload.Loader[E, T]{}, dbx.ErrNilDB
	}
	return relationload.NewWithMappers(
		source.session,
		source.schema,
		source.mapper,
		target.schema,
		target.mapper,
	), nil
}

// LoadBelongsTo loads a belongs-to relation without manually constructing a loader.
func LoadBelongsTo[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	ctx context.Context,
	source *Base[E, S],
	target *Base[T, TS],
	sources *collectionx.List[E],
	relation relationx.BelongsTo[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) error {
	loader, err := Relations(source, target)
	if err != nil {
		return err
	}
	if err := loader.BelongsTo(ctx, sources, relation, assign); err != nil {
		return fmt.Errorf("load belongs-to relation: %w", err)
	}
	return nil
}

// LoadHasOne loads a has-one relation without manually constructing a loader.
func LoadHasOne[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	ctx context.Context,
	source *Base[E, S],
	target *Base[T, TS],
	sources *collectionx.List[E],
	relation relationx.HasOne[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) error {
	loader, err := Relations(source, target)
	if err != nil {
		return err
	}
	if err := loader.HasOne(ctx, sources, relation, assign); err != nil {
		return fmt.Errorf("load has-one relation: %w", err)
	}
	return nil
}

// LoadHasMany loads a has-many relation without manually constructing a loader.
func LoadHasMany[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	ctx context.Context,
	source *Base[E, S],
	target *Base[T, TS],
	sources *collectionx.List[E],
	relation relationx.HasMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) error {
	loader, err := Relations(source, target)
	if err != nil {
		return err
	}
	if err := loader.HasMany(ctx, sources, relation, assign); err != nil {
		return fmt.Errorf("load has-many relation: %w", err)
	}
	return nil
}

// LoadManyToMany loads a many-to-many relation without manually constructing a loader.
func LoadManyToMany[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	ctx context.Context,
	source *Base[E, S],
	target *Base[T, TS],
	sources *collectionx.List[E],
	relation relationx.ManyToMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) error {
	loader, err := Relations(source, target)
	if err != nil {
		return err
	}
	if err := loader.ManyToMany(ctx, sources, relation, assign); err != nil {
		return fmt.Errorf("load many-to-many relation: %w", err)
	}
	return nil
}
