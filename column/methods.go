package column

import (
	"reflect"

	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/lo"
)

func (c Column[E, T]) QueryExpression()              {}
func (c Column[E, T]) QuerySelectItem()              {}
func (c Column[E, T]) ColumnRef() schemax.ColumnMeta { return c.columnRef() }

func (Column[E, T]) ValueType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (c Column[E, T]) Name() string {
	return c.meta.Name
}

func (c Column[E, T]) TableName() string {
	return c.meta.Table
}

func (c Column[E, T]) TableAlias() string {
	return c.meta.Alias
}

func (c Column[E, T]) FieldName() string {
	return c.meta.FieldName
}

func (c Column[E, T]) Meta() schemax.ColumnMeta {
	meta := c.meta
	if meta.References != nil {
		meta.References = new(*meta.References)
	}
	return meta
}

func (c Column[E, T]) IsPrimaryKey() bool {
	return c.meta.PrimaryKey
}

func (c Column[E, T]) IsNullable() bool {
	return c.meta.Nullable
}

func (c Column[E, T]) IsUnique() bool {
	return c.meta.Unique
}

func (c Column[E, T]) IsIndexed() bool {
	return c.meta.Indexed
}

func (c Column[E, T]) DefaultValue() string {
	return c.meta.DefaultValue
}

func (c Column[E, T]) Reference() (schemax.ForeignKeyRef, bool) {
	if c.meta.References == nil {
		return schemax.ForeignKeyRef{}, false
	}
	return *c.meta.References, true
}

func (c Column[E, T]) Ref() string {
	if c.meta.Alias != "" {
		return c.meta.Alias + "." + c.meta.Name
	}
	return c.meta.Table + "." + c.meta.Name
}

func (c Column[E, T]) Eq(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpEq, querydsl.Value(value))
}

func (c Column[E, T]) EqColumn(other Typed[T]) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpEq, columnOperand[T]{Column: other})
}

func (c Column[E, T]) Ne(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpNe, querydsl.Value(value))
}

func (c Column[E, T]) Gt(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpGt, querydsl.Value(value))
}

func (c Column[E, T]) Ge(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpGe, querydsl.Value(value))
}

func (c Column[E, T]) Lt(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpLt, querydsl.Value(value))
}

func (c Column[E, T]) Le(value T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpLe, querydsl.Value(value))
}

func (c Column[E, T]) In(values ...T) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpIn, lo.Map(values, func(value T, _ int) any {
		return value
	}))
}

func (c Column[E, T]) InQuery(query *querydsl.SelectQuery) querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpIn, querydsl.Subquery(query))
}

func (c Column[E, T]) IsNull() querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpIs, nil)
}

func (c Column[E, T]) IsNotNull() querydsl.Predicate {
	return querydsl.Compare(c, querydsl.OpIsNot, nil)
}

func (c Column[E, T]) Set(value T) querydsl.Assignment {
	return columnAssignment[E, T]{
		Column: c,
		Value:  querydsl.Value(value),
	}
}

func (c Column[E, T]) SetColumn(other Typed[T]) querydsl.Assignment {
	return columnAssignment[E, T]{
		Column: c,
		Value:  columnOperand[T]{Column: other},
	}
}

func (c Column[E, T]) SetExcluded() querydsl.Assignment {
	return columnAssignment[E, T]{
		Column: c,
		Value:  excludedColumnOperand[T]{Column: c.columnRef()},
	}
}

func (c Column[E, T]) Asc() querydsl.Order {
	return columnOrder[E, T]{Column: c}
}

func (c Column[E, T]) Desc() querydsl.Order {
	return columnOrder[E, T]{Column: c, Descending: true}
}

func (c Column[E, T]) columnRef() schemax.ColumnMeta {
	return c.meta
}

func (c Column[E, T]) As(alias string) querydsl.SelectItem {
	return querydsl.Alias(c, alias)
}
