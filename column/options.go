package column

import (
	"strings"

	"github.com/arcgolabs/dbx/idgen"
	schemax "github.com/arcgolabs/dbx/schema"
)

func PrimaryKey[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.PrimaryKey = true
		return column
	}
}

func AutoIncrement[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.AutoIncrement = true
		return column
	}
}

func Nullable[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.Nullable = true
		return column
	}
}

func Unique[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.Unique = true
		return column
	}
}

func Indexed[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.Indexed = true
		return column
	}
}

func WithDefault[E any, T any](value string) Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.DefaultValue = value
		return column
	}
}

func WithReference[E any, T any](ref schemax.ForeignKeyRef) Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.References = new(ref)
		return column
	}
}

func WithIDStrategy[E any, T any](strategy idgen.Strategy) Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.IDStrategy = strategy
		return column
	}
}

func WithUUIDVersion[E any, T any](version string) Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.UUIDVersion = strings.TrimSpace(version)
		return column
	}
}

func DBAutoID[E any, T any]() Option[E, T] {
	return WithIDStrategy[E, T](idgen.StrategyDBAuto)
}

func SnowflakeID[E any, T any]() Option[E, T] {
	return WithIDStrategy[E, T](idgen.StrategySnowflake)
}

func UUIDID[E any, T any]() Option[E, T] {
	return WithIDStrategy[E, T](idgen.StrategyUUID)
}

func UUIDv7ID[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.IDStrategy = idgen.StrategyUUID
		column.meta.UUIDVersion = "v7"
		return column
	}
}

func UUIDv4ID[E any, T any]() Option[E, T] {
	return func(column Column[E, T]) Column[E, T] {
		column.meta.IDStrategy = idgen.StrategyUUID
		column.meta.UUIDVersion = "v4"
		return column
	}
}

func (c Column[E, T]) BindColumn(binding schemax.ColumnBinding) any {
	meta := c.meta
	mergeColumnBasic(&meta, binding.Meta)
	mergeColumnFlags(&meta, binding.Meta)
	mergeColumnDefaultsAndRefs(&meta, binding.Meta)
	finalizeColumnIDAndUUID(&meta, binding.Meta)
	c.meta = meta
	return c
}

func mergeColumnBasic(meta *schemax.ColumnMeta, b schemax.ColumnMeta) {
	meta.Name = b.Name
	meta.Table = b.Table
	meta.Alias = b.Alias
	meta.FieldName = b.FieldName
	if meta.GoType == nil {
		meta.GoType = b.GoType
	}
	if meta.SQLType == "" {
		meta.SQLType = b.SQLType
	}
}

func mergeColumnFlags(meta *schemax.ColumnMeta, b schemax.ColumnMeta) {
	meta.PrimaryKey = meta.PrimaryKey || b.PrimaryKey
	if meta.IDStrategy == idgen.StrategyUnset {
		meta.AutoIncrement = meta.AutoIncrement || b.AutoIncrement
	} else {
		meta.AutoIncrement = meta.IDStrategy == idgen.StrategyDBAuto
	}
	meta.Nullable = meta.Nullable || b.Nullable
	meta.Unique = meta.Unique || b.Unique
	meta.Indexed = meta.Indexed || b.Indexed
}

func mergeColumnDefaultsAndRefs(meta *schemax.ColumnMeta, b schemax.ColumnMeta) {
	if meta.DefaultValue == "" {
		meta.DefaultValue = b.DefaultValue
	}
	if meta.References == nil && b.References != nil {
		meta.References = new(*b.References)
	}
}

func finalizeColumnIDAndUUID(meta *schemax.ColumnMeta, b schemax.ColumnMeta) {
	if meta.IDStrategy == idgen.StrategyUnset {
		meta.IDStrategy = b.IDStrategy
	}
	if meta.UUIDVersion == "" {
		meta.UUIDVersion = b.UUIDVersion
	}
	if meta.IDStrategy == idgen.StrategyUUID && meta.UUIDVersion == "" {
		meta.UUIDVersion = idgen.DefaultUUIDVersion
	}
}
