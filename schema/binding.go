package schema

import "reflect"

type ColumnBinding struct {
	Meta ColumnMeta
}

type ColumnBinder interface {
	BindColumn(ColumnBinding) any
}

type ColumnAccessor interface {
	ColumnRef() ColumnMeta
}

type ColumnTypeReporter interface {
	ValueType() reflect.Type
}

type RelationBinding struct {
	Meta RelationMeta
}

type RelationBinder interface {
	BindRelation(RelationBinding) any
	RelationKind() RelationKind
	TargetType() reflect.Type
}
