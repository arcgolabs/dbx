package sqlite

import (
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"
	"slices"
	"strings"

	"github.com/arcgolabs/collectionx"
)

type columnDDLConfig struct {
	AllowAutoIncrement bool
	InlinePrimaryKey   bool
	IncludeReference   bool
}

func (d Dialect) columnDDL(column schemax.ColumnMeta, config columnDDLConfig) (string, error) {
	parts := make([]string, 0, 5)
	parts = append(parts, d.QuoteIdent(column.Name))

	autoIncrementDDL, ok, err := d.sqliteAutoIncrementDDL(column, config)
	if err != nil {
		return "", err
	}
	if ok {
		return strings.Join(append(parts, autoIncrementDDL), " "), nil
	}

	parts = append(parts, resolvedSQLiteType(column))
	parts = append(parts, sqliteColumnConstraintParts(column, config)...)
	parts = append(parts, d.sqliteReferenceParts(column, config.IncludeReference)...)
	return strings.Join(parts, " "), nil
}

func (d Dialect) sqliteAutoIncrementDDL(column schemax.ColumnMeta, config columnDDLConfig) (string, bool, error) {
	if !config.InlinePrimaryKey || !column.AutoIncrement || !config.AllowAutoIncrement {
		return "", false, nil
	}

	typeName := resolvedSQLiteType(column)
	if d.NormalizeType(typeName) != "INTEGER" {
		return "", false, fmt.Errorf("dbx/sqlite: autoincrement requires INTEGER primary key for column %s", column.Name)
	}

	return "INTEGER PRIMARY KEY AUTOINCREMENT", true, nil
}

func sqliteColumnConstraintParts(column schemax.ColumnMeta, config columnDDLConfig) []string {
	parts := make([]string, 0, 3)
	if config.InlinePrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if !column.Nullable && !config.InlinePrimaryKey {
		parts = append(parts, "NOT NULL")
	}
	if column.DefaultValue != "" {
		parts = append(parts, "DEFAULT "+column.DefaultValue)
	}
	return parts
}

func (d Dialect) sqliteReferenceParts(column schemax.ColumnMeta, includeReference bool) []string {
	if !includeReference || column.References == nil {
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

func resolvedSQLiteType(column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	return sqliteType(column)
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
	return joinSQLiteStrings(parts, " ")
}

func (d Dialect) checkDDL(check schemax.CheckMeta) string {
	return "CONSTRAINT " + d.QuoteIdent(check.Name) + " CHECK (" + check.Expression + ")"
}

func sqliteType(column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	if column.GoType == nil {
		return "TEXT"
	}

	typ := dereferenceSQLiteType(column.GoType)
	if isSQLiteTimeType(typ) {
		return "TIMESTAMP"
	}
	if isSQLiteBlobType(typ) {
		return "BLOB"
	}
	if mapped, ok := sqliteKindType(typ.Kind()); ok {
		return mapped
	}
	return fallbackSQLiteType(typ)
}

func sqliteKindType(kind reflect.Kind) (string, bool) {
	switch {
	case kind == reflect.Bool:
		return "BOOLEAN", true
	case slices.Contains(sqliteIntegerKinds, kind):
		return "INTEGER", true
	case slices.Contains(sqliteRealKinds, kind):
		return "REAL", true
	case kind == reflect.String:
		return "TEXT", true
	default:
		return "", false
	}
}

func dereferenceSQLiteType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func isSQLiteTimeType(typ reflect.Type) bool {
	return typ.PkgPath() == "time" && typ.Name() == "Time"
}

func isSQLiteBlobType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8
}

func fallbackSQLiteType(typ reflect.Type) string {
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

func (d Dialect) joinQuotedIdentifiers(items collectionx.List[string]) string {
	if items.Len() == 0 {
		return ""
	}
	quoted := items.Values()
	for index, item := range quoted {
		quoted[index] = d.QuoteIdent(item)
	}
	return strings.Join(quoted, ", ")
}

func joinSQLiteStrings(items collectionx.List[string], sep string) string {
	if items.Len() == 0 {
		return ""
	}
	return strings.Join(items.Values(), sep)
}
