package repository

import (
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	schemax "github.com/arcgolabs/dbx/schema"
)

// EntitySchema is the schema contract required by repository generic code.
type EntitySchema[E any] interface {
	schemax.SchemaSource[E]
	TableName() string
}

// Base provides generic CRUD helpers for a schema-backed entity type.
type Base[E any, S EntitySchema[E]] struct {
	db                  *dbx.DB
	session             dbx.Session
	schema              S
	mapper              mapperx.Mapper[E]
	byIDNotFoundAsError bool
}

// DB returns the database bound to the repository.
func (r *Base[E, S]) DB() *dbx.DB { return r.db }

// Schema returns the schema bound to the repository.
func (r *Base[E, S]) Schema() S { return r.schema }

// Mapper returns the mapper used to persist and scan entities.
func (r *Base[E, S]) Mapper() mapperx.Mapper[E] {
	return r.mapper
}
