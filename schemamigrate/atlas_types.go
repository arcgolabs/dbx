package schemamigrate

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"
	"strings"

	atlasmysql "ariga.io/atlas/sql/mysql"
	atlaspostgres "ariga.io/atlas/sql/postgres"
	atlasschema "ariga.io/atlas/sql/schema"
	atlassqlite "ariga.io/atlas/sql/sqlite"
	collectionx "github.com/arcgolabs/collectionx/list"
)

func atlasFallbackType(rawType string, column schemax.ColumnMeta) atlasschema.Type {
	if rawType == "" {
		rawType = InferTypeName(column)
	}
	typeName := strings.ToLower(strings.TrimSpace(rawType))
	if column.GoType != nil {
		if typ := atlasFallbackFromGoType(rawType, column.GoType); typ != nil {
			return typ
		}
	}
	return atlasFallbackFromTypeName(rawType, typeName)
}

func atlasFallbackFromGoType(rawType string, goType reflect.Type) atlasschema.Type {
	typ := schemax.IndirectGoType(goType)
	if schemax.IsTimeGoType(typ) {
		return &atlasschema.TimeType{T: rawType}
	}
	if typ := atlasFallbackFromBasicGoType(rawType, typ); typ != nil {
		return typ
	}
	return atlasFallbackFromComplexGoType(rawType, typ)
}

func atlasFallbackFromBasicGoType(rawType string, typ reflect.Type) atlasschema.Type {
	kind := typ.Kind()
	if kind == reflect.Bool {
		return &atlasschema.BoolType{T: rawType}
	}
	if kind == reflect.Int64 || schemax.IsSignedIntKind(kind) {
		return &atlasschema.IntegerType{T: rawType}
	}
	if kind == reflect.Uint64 || schemax.IsUnsignedIntKind(kind) {
		return &atlasschema.IntegerType{T: rawType, Unsigned: true}
	}
	if kind == reflect.Float32 || kind == reflect.Float64 {
		return &atlasschema.FloatType{T: rawType}
	}
	if kind == reflect.String {
		return &atlasschema.StringType{T: rawType}
	}
	if schemax.IsByteSliceType(typ) {
		return &atlasschema.BinaryType{T: rawType}
	}
	return nil
}

func atlasFallbackFromComplexGoType(rawType string, typ reflect.Type) atlasschema.Type {
	if !atlasSupportsJSONType(typ) || !strings.Contains(strings.ToLower(rawType), "json") {
		return nil
	}
	return &atlasschema.JSONType{T: rawType}
}

func atlasSupportsJSONType(typ reflect.Type) bool {
	kind := typ.Kind()
	return kind == reflect.Slice || kind == reflect.Map || kind == reflect.Struct
}

func atlasFallbackFromTypeName(rawType, typeName string) atlasschema.Type {
	if typ := atlasFallbackFromBooleanType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromJSONType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromTimeType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromStringType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromBinaryType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromFloatType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromDecimalType(rawType, typeName); typ != nil {
		return typ
	}
	if typ := atlasFallbackFromIntType(rawType, typeName); typ != nil {
		return typ
	}
	return &atlasschema.UnsupportedType{T: rawType}
}

func atlasFallbackFromBooleanType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "bool") {
		return &atlasschema.BoolType{T: rawType}
	}
	return nil
}

func atlasFallbackFromJSONType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "json") {
		return &atlasschema.JSONType{T: rawType}
	}
	return nil
}

func atlasFallbackFromTimeType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "time") || strings.Contains(typeName, "date") {
		return &atlasschema.TimeType{T: rawType}
	}
	return nil
}

func atlasFallbackFromStringType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "char") || strings.Contains(typeName, "text") || strings.Contains(typeName, "string") {
		return &atlasschema.StringType{T: rawType}
	}
	return nil
}

func atlasFallbackFromBinaryType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "blob") || strings.Contains(typeName, "binary") || strings.Contains(typeName, "bytea") {
		return &atlasschema.BinaryType{T: rawType}
	}
	return nil
}

func atlasFallbackFromFloatType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "real") || strings.Contains(typeName, "double") || strings.Contains(typeName, "float") {
		return &atlasschema.FloatType{T: rawType}
	}
	return nil
}

func atlasFallbackFromDecimalType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "numeric") || strings.Contains(typeName, "decimal") {
		return &atlasschema.DecimalType{T: rawType}
	}
	return nil
}

func atlasFallbackFromIntType(rawType, typeName string) atlasschema.Type {
	if strings.Contains(typeName, "int") {
		return &atlasschema.IntegerType{T: rawType}
	}
	return nil
}

func atlasAddAutoIncrementAttr(dialectName string, column *atlasschema.Column) {
	switch strings.ToLower(strings.TrimSpace(dialectName)) {
	case "mysql":
		column.AddAttrs(&atlasmysql.AutoIncrement{})
	case "sqlite":
		column.AddAttrs(&atlassqlite.AutoIncrement{})
	case "postgres":
		column.AddAttrs(&atlaspostgres.Identity{Generation: "BY DEFAULT"})
	}
}

func atlasPrimaryKeyForSpec(table *atlasschema.Table, primaryKey schemax.PrimaryKeyMeta) *atlasschema.Index {
	columns := atlasColumnsByName(table, primaryKey.Columns)
	if len(columns) == 0 {
		return nil
	}
	return atlasschema.NewPrimaryKey(columns...).SetName(primaryKey.Name)
}

func atlasIndexForSpec(table *atlasschema.Table, index schemax.IndexMeta) *atlasschema.Index {
	columns := atlasColumnsByName(table, index.Columns)
	if len(columns) == 0 {
		return nil
	}
	return atlasschema.NewIndex(index.Name).SetUnique(index.Unique).AddColumns(columns...)
}

func atlasColumnsByName(table *atlasschema.Table, names *collectionx.List[string]) []*atlasschema.Column {
	return collectionx.FilterMapList[string, *atlasschema.Column](names, func(_ int, name string) (*atlasschema.Column, bool) {
		column, ok := table.Column(name)
		return column, ok
	}).Values()
}
