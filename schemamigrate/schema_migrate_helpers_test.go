package schemamigrate_test

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
)

func toColumnState(column schemax.ColumnMeta) schemax.ColumnState {
	typeName := column.SQLType
	if typeName == "" {
		typeName = InferTypeNameForTest(column)
	}
	return schemax.ColumnState{
		Name:          column.Name,
		Type:          strings.ToLower(typeName),
		Nullable:      column.Nullable,
		PrimaryKey:    column.PrimaryKey,
		AutoIncrement: column.AutoIncrement,
		DefaultValue:  column.DefaultValue,
	}
}

func toIndexStates(indexes *collectionx.List[schemax.IndexMeta]) *collectionx.List[schemax.IndexState] {
	items := collectionx.NewListWithCapacity[schemax.IndexState](indexes.Len())
	indexes.Range(func(_ int, index schemax.IndexMeta) bool {
		items.Add(schemax.IndexState{
			Name:    index.Name,
			Columns: index.Columns.Clone(),
			Unique:  index.Unique,
		})
		return true
	})
	return items
}

func toForeignKeyStates(foreignKeys *collectionx.List[schemax.ForeignKeyMeta]) *collectionx.List[schemax.ForeignKeyState] {
	items := collectionx.NewListWithCapacity[schemax.ForeignKeyState](foreignKeys.Len())
	foreignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		items.Add(schemax.ForeignKeyState{
			Name:          foreignKey.Name,
			Columns:       foreignKey.Columns.Clone(),
			TargetTable:   foreignKey.TargetTable,
			TargetColumns: foreignKey.TargetColumns.Clone(),
			OnDelete:      foreignKey.OnDelete,
			OnUpdate:      foreignKey.OnUpdate,
		})
		return true
	})
	return items
}

func toCheckStates(checks *collectionx.List[schemax.CheckMeta]) *collectionx.List[schemax.CheckState] {
	items := collectionx.NewListWithCapacity[schemax.CheckState](checks.Len())
	checks.Range(func(_ int, check schemax.CheckMeta) bool {
		items.Add(schemax.CheckState{Name: check.Name, Expression: check.Expression})
		return true
	})
	return items
}
