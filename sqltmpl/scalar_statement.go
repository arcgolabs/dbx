package sqltmpl

import (
	"context"

	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

// ScalarStatement binds one template to concrete parameter and scalar result types.
type ScalarStatement[P any, T any] struct {
	source sqlstmt.TypedSource[P]
}

// NewScalarStatement creates a typed SQL template statement for scalar results.
func NewScalarStatement[P any, T any](template *Template) ScalarStatement[P, T] {
	return ScalarStatement[P, T]{
		source: sqlstmt.For[P](template),
	}
}

// LoadScalarStatement loads a template from registry and wraps it as a typed scalar statement.
func LoadScalarStatement[P any, T any](registry *Registry, name string) (ScalarStatement[P, T], error) {
	template, err := registry.Template(name)
	if err != nil {
		return ScalarStatement[P, T]{}, err
	}
	return NewScalarStatement[P, T](template), nil
}

// MustLoadScalarStatement is LoadScalarStatement and panics on error.
func MustLoadScalarStatement[P any, T any](registry *Registry, name string) ScalarStatement[P, T] {
	statement, err := LoadScalarStatement[P, T](registry, name)
	if err != nil {
		panic(err)
	}
	return statement
}

// StatementName returns the underlying template statement name.
func (s ScalarStatement[P, T]) StatementName() string {
	return s.source.StatementName()
}

// Source returns the typed sqlstmt source.
func (s ScalarStatement[P, T]) Source() sqlstmt.TypedSource[P] {
	return s.source
}

// Bind renders the statement with typed params.
func (s ScalarStatement[P, T]) Bind(params P) (sqlstmt.Bound, error) {
	bound, err := s.source.Bind(params)
	return bound, wrapTypedStatementError("bind scalar statement", err)
}

// Get renders, queries, and expects exactly one scalar value.
func (s ScalarStatement[P, T]) Get(ctx context.Context, session sqlexec.Session, params P) (T, error) {
	value, err := sqlexec.ScalarTyped[P, T](ctx, session, s.source, params)
	return value, wrapTypedStatementError("get scalar statement", err)
}

// Find renders, queries, and returns zero or one scalar value.
func (s ScalarStatement[P, T]) Find(ctx context.Context, session sqlexec.Session, params P) (mo.Option[T], error) {
	value, err := sqlexec.ScalarOptionTyped[P, T](ctx, session, s.source, params)
	return value, wrapTypedStatementError("find scalar statement", err)
}

// Scalar is an alias for Get.
func (s ScalarStatement[P, T]) Scalar(ctx context.Context, session sqlexec.Session, params P) (T, error) {
	return s.Get(ctx, session, params)
}

// ScalarOption is an alias for Find.
func (s ScalarStatement[P, T]) ScalarOption(ctx context.Context, session sqlexec.Session, params P) (mo.Option[T], error) {
	return s.Find(ctx, session, params)
}
