package column

import (
	"reflect"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type Ref[E any, T any] interface {
	Name() string
	refNode()
}

type Accessor interface {
	ColumnRef() schemax.ColumnMeta
}

type TypeReporter interface {
	ValueType() reflect.Type
}

type Typed[T any] interface {
	ColumnRef() schemax.ColumnMeta
}

type Column[E any, T any] struct {
	meta schemax.ColumnMeta
}

// IDColumn declares an ID policy directly in the schema field type.
// The marker strategy is applied during schema binding.
type IDColumn[E any, T any, M idgen.Marker] struct {
	Column[E, T]
}

type Option[E any, T any] func(Column[E, T]) Column[E, T]

func New[E any, T any](opts ...Option[E, T]) Column[E, T] {
	column := Column[E, T]{}
	collectionx.FilterList[Option[E, T]](collectionx.NewList[Option[E, T]](opts...), func(_ int, opt Option[E, T]) bool {
		return opt != nil
	}).Range(func(_ int, opt Option[E, T]) bool {
		column = opt(column)
		return true
	})
	return column
}

func (c IDColumn[E, T, M]) BindColumn(binding schemax.ColumnBinding) any {
	marker := *new(M)
	base := c.Column
	base.meta.PrimaryKey = true
	base.meta.IDStrategy = marker.Strategy()
	if version := marker.UUIDVersion(); version != "" {
		base.meta.UUIDVersion = version
	}
	boundValue := base.BindColumn(binding)
	bound, ok := boundValue.(Column[E, T])
	if !ok {
		return IDColumn[E, T, M]{Column: base}
	}
	return IDColumn[E, T, M]{Column: bound}
}

func Named[T any](source querydsl.TableSource, name string) Column[struct{}, T] {
	table := querydsl.TableRef(source)
	return Column[struct{}, T]{
		meta: schemax.ColumnMeta{
			Name:   strings.TrimSpace(name),
			Table:  table.Name(),
			Alias:  table.Alias(),
			GoType: reflect.TypeFor[T](),
		},
	}
}

func Result[T any](name string) Column[struct{}, T] {
	return Column[struct{}, T]{
		meta: schemax.ColumnMeta{
			Name:   strings.TrimSpace(name),
			GoType: reflect.TypeFor[T](),
		},
	}
}
