package repository

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationload"
)

// Include loads additional data for a list of repository entities.
type Include[E any] interface {
	Load(ctx context.Context, sources *collectionx.List[E]) error
}

// IncludeFunc adapts a function into an Include.
type IncludeFunc[E any] func(ctx context.Context, sources *collectionx.List[E]) error

// Load calls f when it is non-nil.
func (f IncludeFunc[E]) Load(ctx context.Context, sources *collectionx.List[E]) error {
	if f == nil {
		return nil
	}
	return f(ctx, sources)
}

// LoadIncludes applies includes to sources in declaration order.
func LoadIncludes[E any](ctx context.Context, sources *collectionx.List[E], includes ...Include[E]) error {
	for index, include := range includes {
		if include == nil {
			continue
		}
		if err := include.Load(ctx, sources); err != nil {
			return fmt.Errorf("load include %d: %w", index, err)
		}
	}
	return nil
}

// IncludeBelongsTo returns an include for a belongs-to relation.
func IncludeBelongsTo[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	source *Base[E, S],
	target *Base[T, TS],
	relation relationx.BelongsTo[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) Include[E] {
	return IncludeFunc[E](func(ctx context.Context, sources *collectionx.List[E]) error {
		return LoadBelongsTo(ctx, source, target, sources, relation, assign)
	})
}

// IncludeHasOne returns an include for a has-one relation.
func IncludeHasOne[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	source *Base[E, S],
	target *Base[T, TS],
	relation relationx.HasOne[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) Include[E] {
	return IncludeFunc[E](func(ctx context.Context, sources *collectionx.List[E]) error {
		return LoadHasOne(ctx, source, target, sources, relation, assign)
	})
}

// IncludeHasMany returns an include for a has-many relation.
func IncludeHasMany[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	source *Base[E, S],
	target *Base[T, TS],
	relation relationx.HasMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) Include[E] {
	return IncludeFunc[E](func(ctx context.Context, sources *collectionx.List[E]) error {
		return LoadHasMany(ctx, source, target, sources, relation, assign)
	})
}

// IncludeManyToMany returns an include for a many-to-many relation.
func IncludeManyToMany[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	source *Base[E, S],
	target *Base[T, TS],
	relation relationx.ManyToMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) Include[E] {
	return IncludeFunc[E](func(ctx context.Context, sources *collectionx.List[E]) error {
		return LoadManyToMany(ctx, source, target, sources, relation, assign)
	})
}

// IncludeTarget captures source and target repositories for relation include construction.
type IncludeTarget[E any, S EntitySchema[E], T any, TS EntitySchema[T]] struct {
	source *Base[E, S]
	target *Base[T, TS]
}

// IncludeFrom starts relation include construction for source and target repositories.
func IncludeFrom[E any, S EntitySchema[E], T any, TS EntitySchema[T]](
	source *Base[E, S],
	target *Base[T, TS],
) IncludeTarget[E, S, T, TS] {
	return IncludeTarget[E, S, T, TS]{source: source, target: target}
}

// BelongsTo returns an include for a belongs-to relation.
func (b IncludeTarget[E, S, T, TS]) BelongsTo(
	relation relationx.BelongsTo[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) Include[E] {
	return IncludeBelongsTo(b.source, b.target, relation, assign)
}

// HasOne returns an include for a has-one relation.
func (b IncludeTarget[E, S, T, TS]) HasOne(
	relation relationx.HasOne[E, T],
	assign relationload.SingleRelationAssigner[E, T],
) Include[E] {
	return IncludeHasOne(b.source, b.target, relation, assign)
}

// HasMany returns an include for a has-many relation.
func (b IncludeTarget[E, S, T, TS]) HasMany(
	relation relationx.HasMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) Include[E] {
	return IncludeHasMany(b.source, b.target, relation, assign)
}

// ManyToMany returns an include for a many-to-many relation.
func (b IncludeTarget[E, S, T, TS]) ManyToMany(
	relation relationx.ManyToMany[E, T],
	assign relationload.MultiRelationAssigner[E, T],
) Include[E] {
	return IncludeManyToMany(b.source, b.target, relation, assign)
}
