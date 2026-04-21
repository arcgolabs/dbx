package schemamigrate

import (
	"context"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	schemax "github.com/arcgolabs/dbx/schema"
)

func diffSchema(ctx context.Context, schemaDialect Dialect, session dbx.Session, schema Resource) (schemax.TableDiff, error) {
	spec := schema.Spec()
	actual, err := schemaDialect.InspectTable(ctx, session, spec.Name)
	if err != nil {
		return schemax.TableDiff{}, wrapMigrateError("inspect schema table", err)
	}
	if !actual.Exists {
		return missingTableDiff(spec), nil
	}
	return existingTableDiff(schemaDialect, spec, actual), nil
}

func missingTableDiff(spec schemax.TableSpec) schemax.TableDiff {
	diff := newTableDiff(spec.Name)
	diff.MissingTable = true
	diff.MissingColumns = spec.Columns.Clone()
	diff.MissingIndexes = spec.Indexes.Clone()
	diff.MissingForeignKeys = spec.ForeignKeys.Clone()
	diff.MissingChecks = spec.Checks.Clone()
	if spec.PrimaryKey != nil {
		diff.PrimaryKeyDiff = &schemax.PrimaryKeyDiff{
			Expected: new(schemax.ClonePrimaryKeyMeta(*spec.PrimaryKey)),
			Issues:   collectionx.NewList[string]("table does not exist"),
		}
	}
	return diff
}

func existingTableDiff(schemaDialect Dialect, spec schemax.TableSpec, actual schemax.TableState) schemax.TableDiff {
	diff := newTableDiff(spec.Name)
	actualColumns := collectionx.AssociateList[schemax.ColumnState, string, schemax.ColumnState](actual.Columns, func(_ int, column schemax.ColumnState) (string, schemax.ColumnState) {
		return column.Name, column
	})
	diffColumns(schemaDialect, spec.Columns, actualColumns, &diff)
	diffPrimaryKey(spec.PrimaryKey, actual.PrimaryKey, &diff)
	diffIndexes(spec.Indexes, actual.Indexes, &diff)
	diffForeignKeys(spec.ForeignKeys, actual.ForeignKeys, &diff)
	diffChecks(spec.Checks, actual.Checks, &diff)
	return diff
}

func diffColumns(schemaDialect Dialect, expectedColumns collectionx.List[schemax.ColumnMeta], actualColumns collectionx.Map[string, schemax.ColumnState], diff *schemax.TableDiff) {
	missingColumns := collectionx.NewListWithCapacity[schemax.ColumnMeta](expectedColumns.Len())
	columnDiffs := collectionx.NewListWithCapacity[schemax.ColumnDiff](expectedColumns.Len())
	expectedColumns.Range(func(_ int, expected schemax.ColumnMeta) bool {
		actualColumn, ok := actualColumns.Get(expected.Name)
		if !ok {
			missingColumns.Add(expected)
			return true
		}
		issues := columnDiffIssues(schemaDialect, expected, actualColumn)
		if len(issues) == 0 {
			return true
		}
		columnDiffs.Add(schemax.ColumnDiff{Column: expected, Issues: collectionx.NewListWithCapacity[string](len(issues), issues...)})
		return true
	})
	diff.MissingColumns = missingColumns
	diff.ColumnDiffs = columnDiffs
}

func diffPrimaryKey(expected *schemax.PrimaryKeyMeta, actual *schemax.PrimaryKeyState, diff *schemax.TableDiff) {
	issues := primaryKeyIssues(expected, actual)
	if len(issues) == 0 {
		return
	}
	diff.PrimaryKeyDiff = &schemax.PrimaryKeyDiff{
		Expected: clonePrimaryKeyMetaPtr(expected),
		Actual:   clonePrimaryKeyStatePtr(actual),
		Issues:   collectionx.NewListWithCapacity[string](len(issues), issues...),
	}
}

func clonePrimaryKeyMetaPtr(meta *schemax.PrimaryKeyMeta) *schemax.PrimaryKeyMeta {
	if meta == nil {
		return nil
	}
	return new(schemax.ClonePrimaryKeyMeta(*meta))
}

func clonePrimaryKeyStatePtr(state *schemax.PrimaryKeyState) *schemax.PrimaryKeyState {
	if state == nil {
		return nil
	}
	return new(schemax.ClonePrimaryKeyState(*state))
}

func diffIndexes(expected collectionx.List[schemax.IndexMeta], actual collectionx.List[schemax.IndexState], diff *schemax.TableDiff) {
	actualIndexes := collectionx.AssociateList[schemax.IndexState, string, schemax.IndexState](actual, func(_ int, index schemax.IndexState) (string, schemax.IndexState) {
		return indexKey(index.Unique, index.Columns), index
	})
	diff.MissingIndexes = missingByKey(expected, actualIndexes, func(index schemax.IndexMeta) string {
		return indexKey(index.Unique, index.Columns)
	})
}

func diffForeignKeys(expected collectionx.List[schemax.ForeignKeyMeta], actual collectionx.List[schemax.ForeignKeyState], diff *schemax.TableDiff) {
	actualForeignKeys := collectionx.AssociateList[schemax.ForeignKeyState, string, schemax.ForeignKeyState](actual, func(_ int, foreignKey schemax.ForeignKeyState) (string, schemax.ForeignKeyState) {
		return foreignKeyKeyFromState(foreignKey), foreignKey
	})
	diff.MissingForeignKeys = missingByKey(expected, actualForeignKeys, foreignKeyKey)
}

func diffChecks(expected collectionx.List[schemax.CheckMeta], actual collectionx.List[schemax.CheckState], diff *schemax.TableDiff) {
	actualChecks := collectionx.AssociateList[schemax.CheckState, string, schemax.CheckState](actual, func(_ int, check schemax.CheckState) (string, schemax.CheckState) {
		return checkKey(check.Expression), check
	})
	diff.MissingChecks = missingByKey(expected, actualChecks, func(check schemax.CheckMeta) string {
		return checkKey(check.Expression)
	})
}

func missingByKey[T any, S any](expected collectionx.List[T], actual collectionx.Map[string, S], key func(T) string) collectionx.List[T] {
	return collectionx.FilterList[T](expected, func(_ int, item T) bool {
		_, ok := actual.Get(key(item))
		return !ok
	})
}
