package schemamigrate

import (
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"

	atlasschema "ariga.io/atlas/sql/schema"
	"github.com/arcgolabs/collectionx"
	"github.com/samber/lo"
)

func atlasFindIndexMeta(compiled *atlasCompiledTable, index *atlasschema.Index) (schemax.IndexMeta, bool) {
	if compiled == nil || index == nil {
		return schemax.IndexMeta{}, false
	}
	if index.Name != "" {
		if value, ok := compiled.indexesByName.Get(index.Name); ok {
			return value, true
		}
	}
	return compiled.indexesByKey.Get(indexKey(index.Unique, atlasIndexColumns(index)))
}

func atlasFindForeignKeyMeta(compiled *atlasCompiledTable, foreignKey *atlasschema.ForeignKey) (schemax.ForeignKeyMeta, bool) {
	if compiled == nil || foreignKey == nil {
		return schemax.ForeignKeyMeta{}, false
	}
	if foreignKey.Symbol != "" {
		if value, ok := compiled.foreignKeysByName.Get(foreignKey.Symbol); ok {
			return value, true
		}
	}
	return compiled.foreignKeysByKey.Get(atlasForeignKeyKey(foreignKey))
}

func atlasFindCheckMeta(compiled *atlasCompiledTable, check *atlasschema.Check) (schemax.CheckMeta, bool) {
	if compiled == nil || check == nil {
		return schemax.CheckMeta{}, false
	}
	if check.Name != "" {
		if value, ok := compiled.checksByName.Get(check.Name); ok {
			return value, true
		}
	}
	return compiled.checksByExpr.Get(checkKey(check.Expr))
}

func atlasForeignKeyKey(foreignKey *atlasschema.ForeignKey) string {
	columns := collectionx.FilterMapList[*atlasschema.Column, string](collectionx.NewListWithCapacity[*atlasschema.Column](len(foreignKey.Columns), foreignKey.Columns...), func(_ int, column *atlasschema.Column) (string, bool) {
		return column.Name, column != nil
	})
	targetColumns := collectionx.FilterMapList[*atlasschema.Column, string](collectionx.NewListWithCapacity[*atlasschema.Column](len(foreignKey.RefColumns), foreignKey.RefColumns...), func(_ int, column *atlasschema.Column) (string, bool) {
		return column.Name, column != nil
	})
	meta := schemax.ForeignKeyMeta{
		Columns:       columns,
		TargetTable:   lo.If(foreignKey.RefTable != nil, foreignKey.RefTable.Name).Else(""),
		TargetColumns: targetColumns,
		OnDelete:      schemax.ReferentialAction(foreignKey.OnDelete),
		OnUpdate:      schemax.ReferentialAction(foreignKey.OnUpdate),
	}
	return foreignKeyKey(meta)
}

func atlasIndexColumns(index *atlasschema.Index) collectionx.List[string] {
	return collectionx.FilterMapList[*atlasschema.IndexPart, string](collectionx.NewListWithCapacity[*atlasschema.IndexPart](len(index.Parts), index.Parts...), func(_ int, part *atlasschema.IndexPart) (string, bool) {
		if part == nil || part.C == nil {
			return "", false
		}
		return part.C.Name, true
	})
}

func atlasPrimaryKeyState(table *atlasschema.Table) *schemax.PrimaryKeyState {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	return &schemax.PrimaryKeyState{Name: table.PrimaryKey.Name, Columns: atlasIndexColumns(table.PrimaryKey)}
}

func atlasColumnChangeIssue(change atlasschema.ChangeKind) string {
	if change == atlasschema.NoChange {
		return "column migration required"
	}
	return fmt.Sprintf("column migration required (%s)", change)
}
