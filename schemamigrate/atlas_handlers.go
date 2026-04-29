package schemamigrate

import (
	atlasschema "ariga.io/atlas/sql/schema"
	schemax "github.com/arcgolabs/dbx/schema"

	collectionx "github.com/arcgolabs/collectionx/list"
)

type atlasTableChangeHandler func(*schemax.TableDiff, *atlasCompiledTable, *atlasschema.Table, atlasschema.Change) bool

var atlasTableChangeHandlers = []atlasTableChangeHandler{
	handleAtlasAddColumnChange,
	handleAtlasModifyColumnChange,
	handleAtlasRenameColumnChange,
	handleAtlasDropColumnChange,
	handleAtlasAddIndexChange,
	handleAtlasModifyIndexChange,
	handleAtlasRenameIndexChange,
	handleAtlasDropIndexChange,
	handleAtlasAddForeignKeyChange,
	handleAtlasModifyForeignKeyChange,
	handleAtlasDropForeignKeyChange,
	handleAtlasAddCheckChange,
	handleAtlasModifyCheckChange,
	handleAtlasDropCheckChange,
	handleAtlasAddPrimaryKeyChange,
	handleAtlasModifyOrDropPrimaryKeyChange,
}

func atlasApplyTableChangeToDiff(diff *schemax.TableDiff, compiled *atlasCompiledTable, current *atlasschema.Table, change atlasschema.Change) {
	collectionx.NewList(atlasTableChangeHandlers...).Range(func(_ int, handler atlasTableChangeHandler) bool {
		return !handler(diff, compiled, current, change)
	})
}

func handleAtlasAddColumnChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.AddColumn)
	if !ok {
		return false
	}
	atlasHandleAddColumn(diff, compiled, current)
	return true
}

func handleAtlasModifyColumnChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.ModifyColumn)
	if !ok {
		return false
	}
	atlasHandleModifyColumn(diff, compiled, current)
	return true
}

func handleAtlasRenameColumnChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.RenameColumn)
	if !ok {
		return false
	}
	atlasHandleRenameColumn(diff, current)
	return true
}

func handleAtlasDropColumnChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.DropColumn)
	if !ok {
		return false
	}
	atlasHandleDropColumn(diff, current)
	return true
}

func handleAtlasAddIndexChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.AddIndex)
	if !ok {
		return false
	}
	atlasHandleAddIndex(diff, compiled, current)
	return true
}

func handleAtlasModifyIndexChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.ModifyIndex)
	if !ok {
		return false
	}
	atlasHandleModifyIndex(diff, compiled, current)
	return true
}

func handleAtlasRenameIndexChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.RenameIndex)
	if !ok {
		return false
	}
	atlasHandleRenameIndex(diff, current)
	return true
}

func handleAtlasDropIndexChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.DropIndex)
	if !ok {
		return false
	}
	atlasHandleDropIndex(diff, current)
	return true
}

func handleAtlasAddForeignKeyChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.AddForeignKey)
	if !ok {
		return false
	}
	atlasHandleAddForeignKey(diff, compiled, current)
	return true
}

func handleAtlasModifyForeignKeyChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.ModifyForeignKey)
	if !ok {
		return false
	}
	atlasHandleModifyForeignKey(diff, compiled, current)
	return true
}

func handleAtlasDropForeignKeyChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.DropForeignKey)
	if !ok {
		return false
	}
	atlasHandleDropForeignKey(diff, current)
	return true
}

func handleAtlasAddCheckChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.AddCheck)
	if !ok {
		return false
	}
	atlasHandleAddCheck(diff, compiled, current)
	return true
}

func handleAtlasModifyCheckChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.ModifyCheck)
	if !ok {
		return false
	}
	atlasHandleModifyCheck(diff, compiled, current)
	return true
}

func handleAtlasDropCheckChange(diff *schemax.TableDiff, _ *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	current, ok := change.(*atlasschema.DropCheck)
	if !ok {
		return false
	}
	atlasHandleDropCheck(diff, current)
	return true
}

func handleAtlasAddPrimaryKeyChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, _ *atlasschema.Table, change atlasschema.Change) bool {
	_, ok := change.(*atlasschema.AddPrimaryKey)
	if !ok {
		return false
	}
	atlasHandleAddPrimaryKey(diff, compiled)
	return true
}

func handleAtlasModifyOrDropPrimaryKeyChange(diff *schemax.TableDiff, compiled *atlasCompiledTable, current *atlasschema.Table, change atlasschema.Change) bool {
	switch change.(type) {
	case *atlasschema.ModifyPrimaryKey, *atlasschema.DropPrimaryKey:
		atlasHandleModifyOrDropPrimaryKey(diff, compiled, current)
		return true
	default:
		return false
	}
}

func atlasHandleAddColumn(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.AddColumn) {
	if column, ok := compiled.columnsByName.Get(change.C.Name); ok {
		diff.MissingColumns.Add(column)
	}
}

func atlasHandleModifyColumn(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.ModifyColumn) {
	name := change.To.Name
	if name == "" {
		name = change.From.Name
	}
	column, ok := compiled.columnsByName.Get(name)
	if !ok {
		column = schemax.ColumnMeta{Name: name, Table: diff.Table}
	}
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: column, Issues: collectionx.NewList[string](atlasColumnChangeIssue(change.Change))})
}

func atlasHandleRenameColumn(diff *schemax.TableDiff, change *atlasschema.RenameColumn) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.To.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual column rename migration required")})
}

func atlasHandleDropColumn(diff *schemax.TableDiff, change *atlasschema.DropColumn) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.C.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual column removal migration required")})
}

func atlasHandleAddIndex(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.AddIndex) {
	if index, ok := atlasFindIndexMeta(compiled, change.I); ok {
		diff.MissingIndexes.Add(index)
	}
}

func atlasHandleModifyIndex(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.ModifyIndex) {
	if index, ok := atlasFindIndexMeta(compiled, change.To); ok {
		diff.MissingIndexes.Add(index)
		return
	}
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.To.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual index modification required")})
}

func atlasHandleRenameIndex(diff *schemax.TableDiff, change *atlasschema.RenameIndex) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.To.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual index rename migration required")})
}

func atlasHandleDropIndex(diff *schemax.TableDiff, change *atlasschema.DropIndex) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.I.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual index removal migration required")})
}

func atlasHandleAddForeignKey(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.AddForeignKey) {
	if foreignKey, ok := atlasFindForeignKeyMeta(compiled, change.F); ok {
		diff.MissingForeignKeys.Add(foreignKey)
	}
}

func atlasHandleModifyForeignKey(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.ModifyForeignKey) {
	if foreignKey, ok := atlasFindForeignKeyMeta(compiled, change.To); ok {
		diff.MissingForeignKeys.Add(foreignKey)
	}
}

func atlasHandleDropForeignKey(diff *schemax.TableDiff, change *atlasschema.DropForeignKey) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.F.Symbol, Table: diff.Table}, Issues: collectionx.NewList[string]("manual foreign key removal migration required")})
}

func atlasHandleAddCheck(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.AddCheck) {
	if check, ok := atlasFindCheckMeta(compiled, change.C); ok {
		diff.MissingChecks.Add(check)
	}
}

func atlasHandleModifyCheck(diff *schemax.TableDiff, compiled *atlasCompiledTable, change *atlasschema.ModifyCheck) {
	if check, ok := atlasFindCheckMeta(compiled, change.To); ok {
		diff.MissingChecks.Add(check)
	}
}

func atlasHandleDropCheck(diff *schemax.TableDiff, change *atlasschema.DropCheck) {
	diff.ColumnDiffs.Add(schemax.ColumnDiff{Column: schemax.ColumnMeta{Name: change.C.Name, Table: diff.Table}, Issues: collectionx.NewList[string]("manual check removal migration required")})
}

func atlasHandleAddPrimaryKey(diff *schemax.TableDiff, compiled *atlasCompiledTable) {
	diff.PrimaryKeyDiff = &schemax.PrimaryKeyDiff{
		Expected: compiled.spec.PrimaryKey,
		Actual:   atlasPrimaryKeyState(nil),
		Issues:   collectionx.NewList[string]("missing primary key"),
	}
}

func atlasHandleModifyOrDropPrimaryKey(diff *schemax.TableDiff, compiled *atlasCompiledTable, current *atlasschema.Table) {
	var actual *schemax.PrimaryKeyState
	if current != nil {
		actual = atlasPrimaryKeyState(current)
	}
	var expected *schemax.PrimaryKeyMeta
	if compiled.spec.PrimaryKey != nil {
		expected = new(schemax.ClonePrimaryKeyMeta(*compiled.spec.PrimaryKey))
	}
	diff.PrimaryKeyDiff = &schemax.PrimaryKeyDiff{Expected: expected, Actual: actual, Issues: collectionx.NewList[string]("primary key migration required")}
}
