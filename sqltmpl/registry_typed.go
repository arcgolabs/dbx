package sqltmpl

import (
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
)

// Typed loads a template from registry and returns a typed SQL statement source.
func Typed[P any](registry *Registry, name string) (sqlstmt.TypedSource[P], error) {
	template, err := registry.Statement(name)
	if err != nil {
		return sqlstmt.TypedSource[P]{}, err
	}
	return sqlstmt.For[P](template), nil
}

// TypedFor loads a template for d and returns a typed SQL statement source.
func TypedFor[P any](registry *Registry, name string, d dialect.Contract) (sqlstmt.TypedSource[P], error) {
	template, err := registry.StatementFor(name, d)
	if err != nil {
		return sqlstmt.TypedSource[P]{}, err
	}
	return sqlstmt.For[P](template), nil
}

// MustTyped is Typed and panics on error.
func MustTyped[P any](registry *Registry, name string) sqlstmt.TypedSource[P] {
	source, err := Typed[P](registry, name)
	if err != nil {
		panic(err)
	}
	return source
}

// MustTypedFor is TypedFor and panics on error.
func MustTypedFor[P any](registry *Registry, name string, d dialect.Contract) sqlstmt.TypedSource[P] {
	source, err := TypedFor[P](registry, name, d)
	if err != nil {
		panic(err)
	}
	return source
}
