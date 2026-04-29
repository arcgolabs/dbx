package mysql

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"
	"slices"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
)

var mysqlNormalizedTypes = map[string]string{
	"int":        "integer",
	"integer":    "integer",
	"smallint":   "integer",
	"mediumint":  "integer",
	"tinyint":    "integer",
	"bigint":     "bigint",
	"float":      "real",
	"real":       "real",
	"double":     "double",
	"decimal":    "double",
	"numeric":    "double",
	"varchar":    "text",
	"char":       "text",
	"text":       "text",
	"tinytext":   "text",
	"mediumtext": "text",
	"longtext":   "text",
	"blob":       "blob",
	"tinyblob":   "blob",
	"mediumblob": "blob",
	"longblob":   "blob",
	"binary":     "blob",
	"varbinary":  "blob",
	"timestamp":  "timestamp",
	"datetime":   "timestamp",
}

var (
	mysqlIntKinds         = []reflect.Kind{reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32}
	mysqlUnsignedIntKinds = []reflect.Kind{reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32}
)

func mysqlNormalizedTypeName(value string) string {
	typeName := strings.ToLower(strings.TrimSpace(value))
	if strings.HasPrefix(typeName, "tinyint(1)") || typeName == "boolean" || typeName == "bool" {
		return "boolean"
	}

	prefix, _, found := strings.Cut(typeName, "(")
	if found {
		return prefix
	}

	return typeName
}

func (d Dialect) columnDDL(column schemax.ColumnMeta, inlinePrimaryKey, includeReference bool) string {
	parts := []string{
		d.QuoteIdent(column.Name),
		resolvedMySQLType(column),
	}

	parts = append(parts, mysqlColumnConstraintParts(column, inlinePrimaryKey)...)
	if includeReference {
		parts = append(parts, d.mysqlReferenceParts(column)...)
	}

	return strings.Join(parts, " ")
}

func mysqlColumnConstraintParts(column schemax.ColumnMeta, inlinePrimaryKey bool) []string {
	parts := make([]string, 0, 4)
	if column.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	}
	if inlinePrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if !column.Nullable && !inlinePrimaryKey {
		parts = append(parts, "NOT NULL")
	}
	if column.DefaultValue != "" && !column.AutoIncrement {
		parts = append(parts, "DEFAULT "+column.DefaultValue)
	}
	return parts
}

func (d Dialect) mysqlReferenceParts(column schemax.ColumnMeta) []string {
	if column.References == nil {
		return nil
	}

	parts := []string{
		"REFERENCES " + d.QuoteIdent(column.References.TargetTable) + " (" + d.QuoteIdent(column.References.TargetColumn) + ")",
	}
	if column.References.OnDelete != "" {
		parts = append(parts, "ON DELETE "+string(column.References.OnDelete))
	}
	if column.References.OnUpdate != "" {
		parts = append(parts, "ON UPDATE "+string(column.References.OnUpdate))
	}
	return parts
}

func resolvedMySQLType(column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	return mysqlType(column)
}

func (d Dialect) primaryKeyDDL(primaryKey schemax.PrimaryKeyMeta) string {
	return "CONSTRAINT " + d.QuoteIdent(primaryKey.Name) + " PRIMARY KEY (" + d.joinQuotedIdentifiers(primaryKey.Columns) + ")"
}

func (d Dialect) foreignKeyDDL(foreignKey schemax.ForeignKeyMeta) string {
	parts := collectionx.NewList[string]()
	parts.Add("CONSTRAINT " + d.QuoteIdent(foreignKey.Name))
	parts.Add("FOREIGN KEY (" + d.joinQuotedIdentifiers(foreignKey.Columns) + ")")
	parts.Add("REFERENCES " + d.QuoteIdent(foreignKey.TargetTable) + " (" + d.joinQuotedIdentifiers(foreignKey.TargetColumns) + ")")
	if foreignKey.OnDelete != "" {
		parts.Add("ON DELETE " + string(foreignKey.OnDelete))
	}
	if foreignKey.OnUpdate != "" {
		parts.Add("ON UPDATE " + string(foreignKey.OnUpdate))
	}
	return joinMySQLStrings(parts, " ")
}

func (d Dialect) checkDDL(check schemax.CheckMeta) string {
	return "CONSTRAINT " + d.QuoteIdent(check.Name) + " CHECK (" + check.Expression + ")"
}

func mysqlType(column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	if column.GoType == nil {
		return "TEXT"
	}

	typ := dereferenceMySQLType(column.GoType)
	if isMySQLTimeType(typ) {
		return "TIMESTAMP"
	}
	if isMySQLBlobType(typ) {
		return "BLOB"
	}
	if mapped, ok := mysqlKindType(typ.Kind()); ok {
		return mapped
	}
	return fallbackMySQLType(typ)
}

func mysqlKindType(kind reflect.Kind) (string, bool) {
	switch {
	case kind == reflect.Bool:
		return "BOOLEAN", true
	case slices.Contains(mysqlIntKinds, kind):
		return "INT", true
	case kind == reflect.Int64:
		return "BIGINT", true
	case slices.Contains(mysqlUnsignedIntKinds, kind):
		return "INT UNSIGNED", true
	case kind == reflect.Uint64:
		return "BIGINT UNSIGNED", true
	case kind == reflect.Float32:
		return "FLOAT", true
	case kind == reflect.Float64:
		return "DOUBLE", true
	case kind == reflect.String:
		return "TEXT", true
	default:
		return "", false
	}
}

func dereferenceMySQLType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func isMySQLTimeType(typ reflect.Type) bool {
	return typ.PkgPath() == "time" && typ.Name() == "Time"
}

func isMySQLBlobType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8
}

func fallbackMySQLType(typ reflect.Type) string {
	if name := typ.Name(); name != "" {
		return strings.ToUpper(name)
	}
	return "TEXT"
}

func singlePrimaryKeyColumn(primaryKey *schemax.PrimaryKeyMeta) string {
	if primaryKey == nil || primaryKey.Columns.Len() != 1 {
		return ""
	}
	column, _ := primaryKey.Columns.GetFirst()
	return column
}

func (d Dialect) joinQuotedIdentifiers(items *collectionx.List[string]) string {
	if items.Len() == 0 {
		return ""
	}
	return collectionx.MapList[string, string](items, func(_ int, item string) string {
		return d.QuoteIdent(item)
	}).Join(", ")
}

func joinMySQLStrings(items *collectionx.List[string], sep string) string {
	if items.Len() == 0 {
		return ""
	}
	return strings.Join(items.Values(), sep)
}

func referentialAction(value string) schemax.ReferentialAction {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case string(schemax.ReferentialCascade):
		return schemax.ReferentialCascade
	case string(schemax.ReferentialRestrict):
		return schemax.ReferentialRestrict
	case string(schemax.ReferentialSetNull):
		return schemax.ReferentialSetNull
	case string(schemax.ReferentialSetDefault):
		return schemax.ReferentialSetDefault
	case string(schemax.ReferentialNoAction):
		return schemax.ReferentialNoAction
	default:
		return ""
	}
}
