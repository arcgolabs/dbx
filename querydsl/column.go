package querydsl

import (
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	schemax "github.com/arcgolabs/dbx/schema"
)

// Column is a lightweight typed column reference for ad-hoc query sources.
//
// Prefer schema-bound column.Column fields for application tables. Use
// querydsl.Column for CTEs, SQL views, derived tables, and result aliases.
type Column[T any] struct {
	meta schemax.ColumnMeta
}

type columnOrder[T any] struct {
	Column     Column[T]
	Descending bool
}

// Col creates a typed column reference from an ad-hoc source.
func Col[T any](source TableSource, name string) Column[T] {
	table := TableRef(source)
	return Column[T]{
		meta: schemax.ColumnMeta{
			Name:   strings.TrimSpace(name),
			Table:  table.Name(),
			Alias:  table.Alias(),
			GoType: reflect.TypeFor[T](),
		},
	}
}

// Result creates a typed column reference for a select-list alias.
func Result[T any](name string) Column[T] {
	return Column[T]{
		meta: schemax.ColumnMeta{
			Name:   strings.TrimSpace(name),
			GoType: reflect.TypeFor[T](),
		},
	}
}

func (c Column[T]) QueryExpression()              {}
func (c Column[T]) QuerySelectItem()              {}
func (c Column[T]) ColumnRef() schemax.ColumnMeta { return c.meta }
func (c Column[T]) ColumnType(T)                  {}
func (c Column[T]) bindSourceColumn(meta schemax.ColumnMeta) any {
	c.meta = meta
	return c
}

func (Column[T]) ValueType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (c Column[T]) Name() string {
	return c.meta.Name
}

func (c Column[T]) TableName() string {
	return c.meta.Table
}

func (c Column[T]) TableAlias() string {
	return c.meta.Alias
}

func (c Column[T]) Ref() string {
	if c.meta.Alias != "" {
		return c.meta.Alias + "." + c.meta.Name
	}
	if c.meta.Table == "" {
		return c.meta.Name
	}
	return c.meta.Table + "." + c.meta.Name
}

func (c Column[T]) RenderOperand(state *State) (string, error) {
	var builder Buffer
	table := c.meta.Table
	if c.meta.Alias != "" {
		table = c.meta.Alias
	}
	if table != "" {
		builder.WriteString(state.Dialect().QuoteIdent(table))
		builder.WriteRawByte('.')
	}
	builder.WriteString(state.Dialect().QuoteIdent(c.meta.Name))
	return builder.String(), builder.Err("render querydsl column operand")
}

func (c Column[T]) Eq(value T) Predicate { return Compare(c, OpEq, Value(value)) }
func (c Column[T]) Ne(value T) Predicate { return Compare(c, OpNe, Value(value)) }
func (c Column[T]) Gt(value T) Predicate { return Compare(c, OpGt, Value(value)) }
func (c Column[T]) Ge(value T) Predicate { return Compare(c, OpGe, Value(value)) }
func (c Column[T]) Lt(value T) Predicate { return Compare(c, OpLt, Value(value)) }
func (c Column[T]) Le(value T) Predicate { return Compare(c, OpLe, Value(value)) }

func (c Column[T]) EqColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpEq, other)
}

func (c Column[T]) NeColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpNe, other)
}

func (c Column[T]) GtColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpGt, other)
}

func (c Column[T]) GeColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpGe, other)
}

func (c Column[T]) LtColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpLt, other)
}

func (c Column[T]) LeColumn(other TypedColumn[T]) Predicate {
	return Compare(c, OpLe, other)
}

func (c Column[T]) EqQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpEq, query.Subquery())
}

func (c Column[T]) NeQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpNe, query.Subquery())
}

func (c Column[T]) GtQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpGt, query.Subquery())
}

func (c Column[T]) GeQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpGe, query.Subquery())
}

func (c Column[T]) LtQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpLt, query.Subquery())
}

func (c Column[T]) LeQuery(query SelectResult[T]) Predicate {
	return Compare(c, OpLe, query.Subquery())
}

func (c Column[T]) In(values ...T) Predicate {
	return In(c, values...)
}

func (c Column[T]) InList(values *collectionx.List[T]) Predicate {
	return InList(c, values)
}

func (c Column[T]) NotIn(values ...T) Predicate {
	return NotIn(c, values...)
}

func (c Column[T]) NotInList(values *collectionx.List[T]) Predicate {
	return NotInList(c, values)
}

func (c Column[T]) InQuery(query SelectResult[T]) Predicate {
	return InQuery(c, query)
}

func (c Column[T]) NotInQuery(query SelectResult[T]) Predicate {
	return NotInQuery(c, query)
}

func (c Column[T]) Between(lower, upper T) Predicate {
	return Between(c, lower, upper)
}

func (c Column[T]) NotBetween(lower, upper T) Predicate {
	return NotBetween(c, lower, upper)
}

func (c Column[T]) IsNull() Predicate {
	return Compare(c, OpIs, nil)
}

func (c Column[T]) IsNotNull() Predicate {
	return Compare(c, OpIsNot, nil)
}

func (c Column[T]) Asc() Order {
	return columnOrder[T]{Column: c}
}

func (c Column[T]) Desc() Order {
	return columnOrder[T]{Column: c, Descending: true}
}

func (c Column[T]) As(alias string) TypedSelectItem[T] {
	return TypedAlias(c, alias)
}

func (o columnOrder[T]) QueryOrder() {}

func (o columnOrder[T]) RenderOrder(state *State) error {
	state.RenderColumn(o.Column.ColumnRef())
	if o.Descending {
		state.WriteString(" DESC")
		return nil
	}
	state.WriteString(" ASC")
	return nil
}
