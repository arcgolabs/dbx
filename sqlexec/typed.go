package sqlexec

import (
	"context"
	"database/sql"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

// BindTyped binds a typed SQL statement.
func BindTyped[P any](session Session, statement sqlstmt.TypedSource[P], params P) (sqlstmt.Bound, error) {
	exec, err := executor(session)
	if err != nil {
		return sqlstmt.Bound{}, err
	}
	return exec.Bind(statement.Source(), params)
}

// ExecTyped executes a typed SQL statement.
func ExecTyped[P any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P) (sql.Result, error) {
	exec, err := executor(session)
	if err != nil {
		return nil, err
	}
	return exec.Exec(ctx, statement.Source(), params)
}

// QueryTyped queries rows from a typed SQL statement.
func QueryTyped[P any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P) (*sql.Rows, error) {
	exec, err := executor(session)
	if err != nil {
		return nil, err
	}
	return exec.Query(ctx, statement.Source(), params)
}

// ListTyped executes statement and scans all rows using a typed params value.
func ListTyped[P any, E any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	return List(ctx, session, statement.Source(), params, mapper)
}

// QueryListTyped is an alias for ListTyped.
func QueryListTyped[P any, E any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	return ListTyped(ctx, session, statement, params, mapper)
}

// GetTyped executes statement and expects exactly one row using a typed params value.
func GetTyped[P any, E any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P, mapper RowsScanner[E]) (E, error) {
	return Get(ctx, session, statement.Source(), params, mapper)
}

// FindTyped executes statement and returns an optional row using a typed params value.
func FindTyped[P any, E any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P, mapper RowsScanner[E]) (mo.Option[E], error) {
	return Find(ctx, session, statement.Source(), params, mapper)
}

// ScalarTyped executes statement and scans one scalar value using a typed params value.
func ScalarTyped[P any, T any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P) (T, error) {
	return Scalar[T](ctx, session, statement.Source(), params)
}

// ScalarOptionTyped executes statement and scans an optional scalar value using a typed params value.
func ScalarOptionTyped[P any, T any](ctx context.Context, session Session, statement sqlstmt.TypedSource[P], params P) (mo.Option[T], error) {
	return ScalarOption[T](ctx, session, statement.Source(), params)
}
