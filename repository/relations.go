package repository

import (
	"github.com/arcgolabs/dbx"
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
