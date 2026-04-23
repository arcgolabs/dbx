package schemamigrate

import (
	atlasschema "ariga.io/atlas/sql/schema"
	"github.com/arcgolabs/collectionx"
	schemax "github.com/arcgolabs/dbx/schema"
)

func atlasReportFromChanges(changes []atlasschema.Change, compiled *atlasCompiledSchema, current *atlasschema.Schema) schemax.ValidationReport {
	diffs := atlasReportDiffMap(compiled.order)
	currentTables := atlasCurrentTablesByName(current)
	for _, change := range changes {
		atlasApplyChangeToReport(diffs, compiled, currentTables, change)
	}
	return atlasValidationReport(diffs)
}

func atlasReportDiffMap(order collectionx.List[string]) collectionx.OrderedMap[string, *schemax.TableDiff] {
	diffs := collectionx.NewOrderedMapWithCapacity[string, *schemax.TableDiff](order.Len())
	order.Range(func(_ int, name string) bool {
		diffs.Set(name, new(newTableDiff(name)))
		return true
	})
	return diffs
}

func atlasCurrentTablesByName(current *atlasschema.Schema) collectionx.Map[string, *atlasschema.Table] {
	if current == nil {
		return collectionx.NewMap[string, *atlasschema.Table]()
	}
	currentTables := collectionx.NewMapWithCapacity[string, *atlasschema.Table](len(current.Tables))
	for _, table := range current.Tables {
		currentTables.Set(table.Name, table)
	}
	return currentTables
}

func atlasApplyChangeToReport(diffs collectionx.OrderedMap[string, *schemax.TableDiff], compiled *atlasCompiledSchema, currentTables collectionx.Map[string, *atlasschema.Table], change atlasschema.Change) {
	switch c := change.(type) {
	case *atlasschema.AddTable:
		atlasApplyAddTableChange(diffs, compiled, c)
	case *atlasschema.ModifyTable:
		atlasApplyModifyTableChange(diffs, compiled, currentTables, c)
	}
}

func atlasApplyAddTableChange(diffs collectionx.OrderedMap[string, *schemax.TableDiff], compiled *atlasCompiledSchema, change *atlasschema.AddTable) {
	compiledTable, ok := compiled.tables.Get(change.T.Name)
	if !ok {
		return
	}
	diff, _ := diffs.Get(change.T.Name)
	diff.MissingTable = true
	diff.MissingColumns = compiledTable.spec.Columns.Clone()
	diff.MissingIndexes = compiledTable.spec.Indexes.Clone()
	diff.MissingForeignKeys = compiledTable.spec.ForeignKeys.Clone()
	diff.MissingChecks = compiledTable.spec.Checks.Clone()
	if compiledTable.spec.PrimaryKey != nil {
		diff.PrimaryKeyDiff = &schemax.PrimaryKeyDiff{
			Expected: new(schemax.ClonePrimaryKeyMeta(*compiledTable.spec.PrimaryKey)),
			Issues:   collectionx.NewList[string]("table does not exist"),
		}
	}
}

func atlasApplyModifyTableChange(diffs collectionx.OrderedMap[string, *schemax.TableDiff], compiled *atlasCompiledSchema, currentTables collectionx.Map[string, *atlasschema.Table], change *atlasschema.ModifyTable) {
	compiledTable, ok := compiled.tables.Get(change.T.Name)
	if !ok {
		return
	}
	diff, _ := diffs.Get(change.T.Name)
	currentTable, _ := currentTables.Get(change.T.Name)
	for _, tableChange := range change.Changes {
		atlasApplyTableChangeToDiff(diff, compiledTable, currentTable, tableChange)
	}
}

func atlasValidationReport(diffs collectionx.OrderedMap[string, *schemax.TableDiff]) schemax.ValidationReport {
	items := collectionx.NewListWithCapacity[schemax.TableDiff](diffs.Len())
	diffs.Range(func(_ string, diff *schemax.TableDiff) bool {
		items.Add(*diff)
		return true
	})
	return schemax.ValidationReport{
		Tables:   items,
		Backend:  schemax.ValidationBackendAtlas,
		Complete: true,
	}
}
