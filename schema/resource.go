package schema

import (
	"reflect"
	"strings"
)

// Resource exposes a bound schema as migration-ready table metadata.
type Resource interface {
	TableName() string
	TableAlias() string
	Spec() TableSpec
}

func CloneColumnMeta(meta ColumnMeta) ColumnMeta {
	if meta.References == nil {
		return meta
	}
	meta.References = new(*meta.References)
	return meta
}

func CloneIndexMeta(meta IndexMeta) IndexMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func ClonePrimaryKeyMeta(meta PrimaryKeyMeta) PrimaryKeyMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func CloneForeignKeyMeta(meta ForeignKeyMeta) ForeignKeyMeta {
	meta.Columns = meta.Columns.Clone()
	meta.TargetColumns = meta.TargetColumns.Clone()
	return meta
}

func CloneCheckMeta(meta CheckMeta) CheckMeta {
	return meta
}

func ClonePrimaryKeyState(state PrimaryKeyState) PrimaryKeyState {
	state.Columns = state.Columns.Clone()
	return state
}

func InferTypeName(column ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	return InferTypeNameFromGoType(column.GoType)
}

func InferTypeNameFromGoType(goType reflect.Type) string {
	if goType == nil {
		return ""
	}
	typ := IndirectGoType(goType)
	if IsTimeGoType(typ) {
		return "timestamp"
	}
	if typeName, ok := inferBasicTypeName(typ); ok {
		return typeName
	}
	if IsByteSliceType(typ) {
		return "blob"
	}
	return strings.ToLower(typ.Name())
}

func IndirectGoType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func IsTimeGoType(typ reflect.Type) bool {
	return typ.PkgPath() == "time" && typ.Name() == "Time"
}

func inferBasicTypeName(typ reflect.Type) (string, bool) {
	kind := typ.Kind()
	if kind == reflect.Bool {
		return "boolean", true
	}
	if IsSignedIntKind(kind) {
		return "integer", true
	}
	if kind == reflect.Int64 {
		return "bigint", true
	}
	if IsUnsignedIntKind(kind) {
		return "integer", true
	}
	if kind == reflect.Uint64 {
		return "bigint", true
	}
	if kind == reflect.Float32 {
		return "real", true
	}
	if kind == reflect.Float64 {
		return "double", true
	}
	if kind == reflect.String {
		return "text", true
	}
	return "", false
}

func IsSignedIntKind(kind reflect.Kind) bool {
	return kind == reflect.Int ||
		kind == reflect.Int8 ||
		kind == reflect.Int16 ||
		kind == reflect.Int32
}

func IsUnsignedIntKind(kind reflect.Kind) bool {
	return kind == reflect.Uint ||
		kind == reflect.Uint8 ||
		kind == reflect.Uint16 ||
		kind == reflect.Uint32
}

func IsByteSliceType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8
}
