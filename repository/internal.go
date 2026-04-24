package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"maps"
	"slices"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

type countRow struct {
	Count int64 `dbx:"count"`
}

func (r *Base[E, S]) defaultSelect() *querydsl.SelectQuery {
	items := collectionx.MapList[mapperx.MappedField, querydsl.SelectItem](r.mapper.Fields(), func(_ int, field mapperx.MappedField) querydsl.SelectItem {
		return columnx.Named[any](r.schema, field.Column)
	})
	return querydsl.SelectList(items).From(r.schema)
}

func (r *Base[E, S]) applySpecs(specs ...Spec) *querydsl.SelectQuery {
	query := r.defaultSelect()
	collectionx.NewList[Spec](specs...).Range(func(_ int, spec Spec) bool {
		if spec != nil {
			query = spec.Apply(query)
		}
		return true
	})
	return query
}

func cloneForCount(query *querydsl.SelectQuery) *querydsl.SelectQuery {
	cloned := query.Clone()
	if cloned == nil {
		return nil
	}
	cloned.Orders = nil
	cloned.LimitN = nil
	cloned.OffsetN = nil
	return cloned
}

func countQueryRequiresWrap(query *querydsl.SelectQuery) bool {
	return queryRequiresProjectionWrap(query)
}

func queryRequiresProjectionWrap(query *querydsl.SelectQuery) bool {
	return query != nil &&
		(query.Distinct || query.Groups.Len() > 0 || query.HavingExp != nil || query.Unions.Len() > 0)
}

func wrappedCountBound(session dbx.Session, query *querydsl.SelectQuery) (sqlstmt.Bound, error) {
	source := cloneForCount(query)
	bound, err := dbx.Build(session, source)
	if err != nil {
		return sqlstmt.Bound{}, fmt.Errorf("build wrapped count query: %w", err)
	}
	quotedCount := session.Dialect().QuoteIdent("count")
	quotedAlias := session.Dialect().QuoteIdent("dbx_count_source")
	return sqlstmt.Bound{
		SQL:          "SELECT COUNT(*) AS " + quotedCount + " FROM (" + bound.SQL + ") AS " + quotedAlias,
		Args:         bound.Args,
		CapacityHint: 1,
	}, nil
}

func (r *Base[E, S]) existsBound(query *querydsl.SelectQuery) (sqlstmt.Bound, error) {
	if queryRequiresProjectionWrap(query) {
		return wrappedExistsBound(r.session, query)
	}
	source, err := r.existsSelect(query)
	if err != nil {
		return sqlstmt.Bound{}, err
	}
	bound, err := dbx.Build(r.session, source)
	if err != nil {
		return sqlstmt.Bound{}, fmt.Errorf("build exists query: %w", err)
	}
	return bound, nil
}

func (r *Base[E, S]) existsSelect(query *querydsl.SelectQuery) (*querydsl.SelectQuery, error) {
	item, err := r.existsSelectItem()
	if err != nil {
		return nil, err
	}
	source := cloneOrDefault(r, query)
	source.Items = collectionx.NewList[querydsl.SelectItem](item)
	source.Orders = nil
	source.LimitN = nil
	source.Limit(1)
	return source, nil
}

func (r *Base[E, S]) existsSelectItem() (querydsl.SelectItem, error) {
	field, ok := r.mapper.Fields().GetFirst()
	if !ok {
		return nil, fmt.Errorf("dbx: repository %s mapper has no selectable fields", r.schema.TableName())
	}
	return columnx.Named[any](r.schema, field.Column), nil
}

func wrappedExistsBound(session dbx.Session, query *querydsl.SelectQuery) (sqlstmt.Bound, error) {
	source := cloneForExists(query)
	bound, err := dbx.Build(session, source)
	if err != nil {
		return sqlstmt.Bound{}, fmt.Errorf("build wrapped exists query: %w", err)
	}
	quotedAlias := session.Dialect().QuoteIdent("dbx_exists_source")
	return sqlstmt.Bound{
		SQL:          "SELECT 1 FROM (" + bound.SQL + ") AS " + quotedAlias + " LIMIT 1",
		Args:         bound.Args,
		CapacityHint: 1,
	}, nil
}

func cloneForExists(query *querydsl.SelectQuery) *querydsl.SelectQuery {
	cloned := query.Clone()
	if cloned == nil {
		return nil
	}
	cloned.Orders = nil
	cloned.LimitN = nil
	cloned.Limit(1)
	return cloned
}

func queryExistsBound(ctx context.Context, session dbx.Session, bound sqlstmt.Bound) (exists bool, err error) {
	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		return false, fmt.Errorf("query exists rows: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close exists rows: %w", closeErr))
		}
	}()

	exists = rows.Next()
	if exists {
		var discard any
		if scanErr := rows.Scan(&discard); scanErr != nil {
			return false, fmt.Errorf("scan exists row: %w", scanErr)
		}
	}
	if iterErr := rows.Err(); iterErr != nil {
		return false, fmt.Errorf("iterate exists rows: %w", iterErr)
	}
	return exists, nil
}

func firstCount(rows collectionx.List[countRow]) int64 {
	if rows.IsEmpty() {
		return 0
	}
	row, _ := rows.GetFirst()
	return row.Count
}

func cloneOrDefault[E any, S EntitySchema[E]](r *Base[E, S], query *querydsl.SelectQuery) *querydsl.SelectQuery {
	if query == nil {
		return r.defaultSelect()
	}
	return query.Clone()
}

func optionFromResult[T any](item T, err error) (mo.Option[T], error) {
	if err == nil {
		return mo.Some(item), nil
	}
	if errors.Is(err, ErrNotFound) {
		return mo.None[T](), nil
	}
	return mo.None[T](), err
}

func (r *Base[E, S]) primaryColumnName() string {
	type primaryColumnProvider interface {
		PrimaryColumn() (schemax.ColumnMeta, bool)
	}
	if provider, ok := any(r.schema).(primaryColumnProvider); ok {
		if column, ok := provider.PrimaryColumn(); ok && column.Name != "" {
			return column.Name
		}
	}
	return "id"
}

func (r *Base[E, S]) primaryKeyColumns() collectionx.List[string] {
	type primaryKeyProvider interface {
		PrimaryKey() (schemax.PrimaryKeyMeta, bool)
	}
	if provider, ok := any(r.schema).(primaryKeyProvider); ok {
		if primary, ok := provider.PrimaryKey(); ok && primary.Columns.Len() > 0 {
			return primary.Columns.Clone()
		}
	}
	return collectionx.NewList[string](r.primaryColumnName())
}

func keyPredicate[S querydsl.TableSource](schema S, key Key) querydsl.Predicate {
	if len(key) == 0 {
		return nil
	}
	columns := collectionx.NewList[string](slices.Sorted(maps.Keys(key))...)
	predicates := collectionx.MapList[string, querydsl.Predicate](columns, func(_ int, column string) querydsl.Predicate {
		return columnx.Named[any](schema, column).Eq(key[column])
	})
	return querydsl.AndList(predicates)
}

func hasAffectedRows(result sql.Result) bool {
	if result == nil {
		return false
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false
	}
	return rows > 0
}

func wrapMutationError(err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "unique") || strings.Contains(message, "duplicate") || strings.Contains(message, "constraint") {
		return &ConflictError{Err: err}
	}
	return err
}
