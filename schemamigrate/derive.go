package schemamigrate

import (
	"strings"

	schemax "github.com/arcgolabs/dbx/schema"
)

func normalizeExpectedType(schemaDialect Dialect, column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return schemaDialect.NormalizeType(column.SQLType)
	}
	return schemaDialect.NormalizeType(InferTypeName(column))
}

func InferTypeName(column schemax.ColumnMeta) string {
	return schemax.InferTypeName(column)
}

func normalizeDefault(value string) string {
	return strings.TrimSpace(strings.Trim(value, "()"))
}

func normalizeCheckExpression(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}
