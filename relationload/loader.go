package relationload

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	relationx "github.com/arcgolabs/dbx/relation"
	schemax "github.com/arcgolabs/dbx/schema"
)

// Loader binds the repeated relation loading dependencies for one source-target pair.
type Loader[S any, T any] struct {
	session      dbx.Session
	sourceSchema schemax.SchemaSource[S]
	sourceMapper mapperx.Mapper[S]
	targetSchema schemax.SchemaSource[T]
	targetMapper mapperx.Mapper[T]
}

// New constructs a relation loader and derives mappers from the provided schemas.
func New[S any, T any](session dbx.Session, sourceSchema schemax.SchemaSource[S], targetSchema schemax.SchemaSource[T]) Loader[S, T] {
	return NewWithMappers(session, sourceSchema, mapperx.MustMapper[S](sourceSchema), targetSchema, mapperx.MustMapper[T](targetSchema))
}

// NewWithMappers constructs a relation loader with explicit mappers.
func NewWithMappers[S any, T any](
	session dbx.Session,
	sourceSchema schemax.SchemaSource[S],
	sourceMapper mapperx.Mapper[S],
	targetSchema schemax.SchemaSource[T],
	targetMapper mapperx.Mapper[T],
) Loader[S, T] {
	return Loader[S, T]{
		session:      session,
		sourceSchema: sourceSchema,
		sourceMapper: sourceMapper,
		targetSchema: targetSchema,
		targetMapper: targetMapper,
	}
}

// BelongsTo loads a belongs-to relation into the source list.
func (l Loader[S, T]) BelongsTo(ctx context.Context, sources *collectionx.List[S], relation relationx.BelongsTo[S, T], assign SingleRelationAssigner[S, T]) error {
	return LoadBelongsTo(ctx, l.session, sources, l.sourceSchema, l.sourceMapper, relation, l.targetSchema, l.targetMapper, assign)
}

// HasOne loads a has-one relation into the source list.
func (l Loader[S, T]) HasOne(ctx context.Context, sources *collectionx.List[S], relation relationx.HasOne[S, T], assign SingleRelationAssigner[S, T]) error {
	return LoadHasOne(ctx, l.session, sources, l.sourceSchema, l.sourceMapper, relation, l.targetSchema, l.targetMapper, assign)
}

// HasMany loads a has-many relation into the source list.
func (l Loader[S, T]) HasMany(ctx context.Context, sources *collectionx.List[S], relation relationx.HasMany[S, T], assign MultiRelationAssigner[S, T]) error {
	return LoadHasMany(ctx, l.session, sources, l.sourceSchema, l.sourceMapper, relation, l.targetSchema, l.targetMapper, assign)
}

// ManyToMany loads a many-to-many relation into the source list.
func (l Loader[S, T]) ManyToMany(ctx context.Context, sources *collectionx.List[S], relation relationx.ManyToMany[S, T], assign MultiRelationAssigner[S, T]) error {
	return LoadManyToMany(ctx, l.session, sources, l.sourceSchema, l.sourceMapper, relation, l.targetSchema, l.targetMapper, assign)
}
