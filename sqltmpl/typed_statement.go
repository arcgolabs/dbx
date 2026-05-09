package sqltmpl

import (
	"context"
	"database/sql"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

// Statement binds one template to concrete parameter and result row types.
type Statement[P any, R any] struct {
	source sqlstmt.TypedSource[P]
	mapper sqlexec.RowsScanner[R]
}

// NewStatement creates a typed SQL template statement with an explicit row mapper.
func NewStatement[P any, R any](template *Template, mapper sqlexec.RowsScanner[R]) Statement[P, R] {
	return Statement[P, R]{
		source: sqlstmt.For[P](template),
		mapper: mapper,
	}
}

// NewStructStatement creates a typed SQL template statement using the default struct mapper.
func NewStructStatement[P any, R any](template *Template) Statement[P, R] {
	return NewStatement[P, R](template, mapperx.MustStructMapper[R]())
}

// LoadStatement loads a template from registry and wraps it as a typed statement.
func LoadStatement[P any, R any](registry *Registry, name string, mapper sqlexec.RowsScanner[R]) (Statement[P, R], error) {
	template, err := registry.Template(name)
	if err != nil {
		return Statement[P, R]{}, err
	}
	return NewStatement[P, R](template, mapper), nil
}

// LoadStatementFor loads a template for d and wraps it as a typed statement.
func LoadStatementFor[P any, R any](registry *Registry, name string, d dialect.Contract, mapper sqlexec.RowsScanner[R]) (Statement[P, R], error) {
	template, err := registry.TemplateFor(name, d)
	if err != nil {
		return Statement[P, R]{}, err
	}
	return NewStatement[P, R](template, mapper), nil
}

// LoadStructStatement loads a template from registry and wraps it with the default struct mapper.
func LoadStructStatement[P any, R any](registry *Registry, name string) (Statement[P, R], error) {
	return LoadStatement[P, R](registry, name, mapperx.MustStructMapper[R]())
}

// LoadStructStatementFor loads a template for d and wraps it with the default struct mapper.
func LoadStructStatementFor[P any, R any](registry *Registry, name string, d dialect.Contract) (Statement[P, R], error) {
	return LoadStatementFor[P, R](registry, name, d, mapperx.MustStructMapper[R]())
}

// MustLoadStatement is LoadStatement and panics on error.
func MustLoadStatement[P any, R any](registry *Registry, name string, mapper sqlexec.RowsScanner[R]) Statement[P, R] {
	statement, err := LoadStatement[P, R](registry, name, mapper)
	if err != nil {
		panic(err)
	}
	return statement
}

// MustLoadStatementFor is LoadStatementFor and panics on error.
func MustLoadStatementFor[P any, R any](registry *Registry, name string, d dialect.Contract, mapper sqlexec.RowsScanner[R]) Statement[P, R] {
	statement, err := LoadStatementFor[P, R](registry, name, d, mapper)
	if err != nil {
		panic(err)
	}
	return statement
}

// MustLoadStructStatement is LoadStructStatement and panics on error.
func MustLoadStructStatement[P any, R any](registry *Registry, name string) Statement[P, R] {
	return MustLoadStatement[P, R](registry, name, mapperx.MustStructMapper[R]())
}

// MustLoadStructStatementFor is LoadStructStatementFor and panics on error.
func MustLoadStructStatementFor[P any, R any](registry *Registry, name string, d dialect.Contract) Statement[P, R] {
	return MustLoadStatementFor[P, R](registry, name, d, mapperx.MustStructMapper[R]())
}

// StatementName returns the underlying template statement name.
func (s Statement[P, R]) StatementName() string {
	return s.source.StatementName()
}

// Source returns the typed sqlstmt source.
func (s Statement[P, R]) Source() sqlstmt.TypedSource[P] {
	return s.source
}

// Bind renders the statement with typed params.
func (s Statement[P, R]) Bind(params P) (sqlstmt.Bound, error) {
	bound, err := s.source.Bind(params)
	return bound, wrapTypedStatementError("bind typed statement", err)
}

// Exec renders and executes the statement.
func (s Statement[P, R]) Exec(ctx context.Context, session sqlexec.Session, params P) (sql.Result, error) {
	result, err := sqlexec.ExecTyped(ctx, session, s.source, params)
	return result, wrapTypedStatementError("execute typed statement", err)
}

// Query renders and queries rows from the statement.
func (s Statement[P, R]) Query(ctx context.Context, session sqlexec.Session, params P) (*sql.Rows, error) {
	rows, err := sqlexec.QueryTyped(ctx, session, s.source, params)
	return rows, wrapTypedStatementError("query typed statement", err)
}

// List renders, queries, and scans all rows.
func (s Statement[P, R]) List(ctx context.Context, session sqlexec.Session, params P) (*collectionx.List[R], error) {
	rows, err := sqlexec.ListTyped(ctx, session, s.source, params, s.mapper)
	return rows, wrapTypedStatementError("list typed statement", err)
}

// Get renders, queries, and expects exactly one row.
func (s Statement[P, R]) Get(ctx context.Context, session sqlexec.Session, params P) (R, error) {
	row, err := sqlexec.GetTyped(ctx, session, s.source, params, s.mapper)
	return row, wrapTypedStatementError("get typed statement", err)
}

// Find renders, queries, and returns zero or one row.
func (s Statement[P, R]) Find(ctx context.Context, session sqlexec.Session, params P) (mo.Option[R], error) {
	row, err := sqlexec.FindTyped(ctx, session, s.source, params, s.mapper)
	return row, wrapTypedStatementError("find typed statement", err)
}

// Scalar renders, queries, and scans one scalar value into R.
func (s Statement[P, R]) Scalar(ctx context.Context, session sqlexec.Session, params P) (R, error) {
	value, err := sqlexec.ScalarTyped[P, R](ctx, session, s.source, params)
	return value, wrapTypedStatementError("scalar typed statement", err)
}

// ScalarOption renders, queries, and scans zero or one scalar value into R.
func (s Statement[P, R]) ScalarOption(ctx context.Context, session sqlexec.Session, params P) (mo.Option[R], error) {
	value, err := sqlexec.ScalarOptionTyped[P, R](ctx, session, s.source, params)
	return value, wrapTypedStatementError("scalar option typed statement", err)
}

func wrapTypedStatementError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/sqltmpl: %s: %w", op, err)
}
