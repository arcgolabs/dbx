package dbx_test

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	relationx "github.com/arcgolabs/dbx/relation"
	relationload "github.com/arcgolabs/dbx/relationload"
)

func LoadBelongsTo[S any, T any](ctx context.Context, session Session, sources *collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.BelongsTo[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.SingleRelationAssigner[S, T]) error {
	if err := relationload.LoadBelongsTo(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load belongs-to relation: %w", err)
	}
	return nil
}

func LoadHasMany[S any, T any](ctx context.Context, session Session, sources *collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.HasMany[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.MultiRelationAssigner[S, T]) error {
	if err := relationload.LoadHasMany(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load has-many relation: %w", err)
	}
	return nil
}

func LoadHasOne[S any, T any](ctx context.Context, session Session, sources *collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.HasOne[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.SingleRelationAssigner[S, T]) error {
	if err := relationload.LoadHasOne(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load has-one relation: %w", err)
	}
	return nil
}

func LoadManyToMany[S any, T any](ctx context.Context, session Session, sources *collectionx.List[S], sourceSchema SchemaSource[S], sourceMapper Mapper[S], relation relationx.ManyToMany[S, T], targetSchema SchemaSource[T], targetMapper Mapper[T], assign relationload.MultiRelationAssigner[S, T]) error {
	if err := relationload.LoadManyToMany(ctx, session, sources, sourceSchema, sourceMapper, relation, targetSchema, targetMapper, assign); err != nil {
		return fmt.Errorf("load many-to-many relation: %w", err)
	}
	return nil
}
