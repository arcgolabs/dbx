package schemamigrate

import (
	atlasschema "ariga.io/atlas/sql/schema"
	schemax "github.com/arcgolabs/dbx/schema"
)

type AtlasCompiledSchemaTestView struct {
	Schema *atlasschema.Schema
	tables map[string]*atlasschema.Table
}

func CompileAtlasSchemaForTest(dialectName string, schemas ...Resource) *AtlasCompiledSchemaTestView {
	compiled := compileAtlasSchema(dialectName, nil, "main", schemas)
	if compiled == nil {
		return nil
	}
	view := &AtlasCompiledSchemaTestView{
		Schema: compiled.schema,
		tables: make(map[string]*atlasschema.Table, compiled.tables.Len()),
	}
	compiled.tables.Range(func(name string, table *atlasCompiledTable) bool {
		view.tables[name] = table.table
		return true
	})
	return view
}

func (v *AtlasCompiledSchemaTestView) Table(name string) (*atlasschema.Table, bool) {
	if v == nil {
		return nil, false
	}
	table, ok := v.tables[name]
	return table, ok
}

func AtlasSplitChangesForTest(changes []atlasschema.Change) ([]atlasschema.Change, []schemax.MigrationAction) {
	return atlasSplitChanges(changes)
}
