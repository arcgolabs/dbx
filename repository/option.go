package repository

import (
	"context"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// GetByIDOption returns the entity identified by the primary key as an option.
func (r *Base[E, S]) GetByIDOption(ctx context.Context, id any) (mo.Option[E], error) {
	return optionFromResult(r.GetByID(ctx, id))
}

// GetByKeyOption returns the entity identified by the provided key as an option.
func (r *Base[E, S]) GetByKeyOption(ctx context.Context, key Key) (mo.Option[E], error) {
	return optionFromResult(r.GetByKey(ctx, key))
}

// FirstOption returns the first matching entity as an option.
func (r *Base[E, S]) FirstOption(ctx context.Context, query *querydsl.SelectQuery) (mo.Option[E], error) {
	return optionFromResult(r.First(ctx, query))
}

// FirstSpecOption returns the first entity matched by the provided specs as an option.
func (r *Base[E, S]) FirstSpecOption(ctx context.Context, specs ...Spec) (mo.Option[E], error) {
	return r.FirstOption(ctx, r.applySpecs(specs...))
}
