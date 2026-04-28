package schemamigrate

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	"github.com/arcgolabs/collectionx"
)

func mappedMigrationActions[T any](items collectionx.List[T], mapper func(T) schemax.MigrationAction) collectionx.List[schemax.MigrationAction] {
	return collectionx.MapList[T, schemax.MigrationAction](items, func(_ int, item T) schemax.MigrationAction {
		return mapper(item)
	})
}

func columnDiffManualActions(diff schemax.TableDiff) collectionx.List[schemax.MigrationAction] {
	return mappedMigrationActions(diff.ColumnDiffs, func(cd schemax.ColumnDiff) schemax.MigrationAction {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   diff.Table,
			Summary: "manual column migration required for " + cd.Column.Name + ": " + cd.Issues.Join("; "),
		}
	})
}

func columnDiffIssues(schemaDialect Dialect, expected schemax.ColumnMeta, actual schemax.ColumnState) []string {
	expectedType := normalizeExpectedType(schemaDialect, expected)
	actualType := schemaDialect.NormalizeType(actual.Type)
	return collectionx.FilterMapList[string, string](collectionx.NewList[string](
		typeMismatchIssue(expectedType, actualType),
		nullableMismatchIssue(expected, actual),
		autoIncrementMismatchIssue(expected, actual),
		defaultMismatchIssue(expected, actual),
	), func(_ int, issue string) (string, bool) {
		return issue, issue != ""
	}).Values()
}

func buildCreateTableAction(schemaDialect Dialect, spec schemax.TableSpec) schemax.MigrationAction {
	statement, err := schemaDialect.BuildCreateTable(spec)
	if err != nil {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   spec.Name,
			Summary: "manual create table migration required: " + err.Error(),
		}
	}
	return schemax.MigrationAction{
		Kind:       schemax.MigrationActionCreateTable,
		Table:      spec.Name,
		Summary:    "create table " + spec.Name,
		Statement:  statement,
		Executable: true,
	}
}

func buildAddColumnAction(schemaDialect Dialect, table string, column schemax.ColumnMeta) schemax.MigrationAction {
	statement, err := schemaDialect.BuildAddColumn(table, column)
	if err != nil {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   table,
			Summary: "manual add column migration required for " + column.Name + ": " + err.Error(),
		}
	}
	return schemax.MigrationAction{
		Kind:       schemax.MigrationActionAddColumn,
		Table:      table,
		Summary:    "add column " + column.Name,
		Statement:  statement,
		Executable: true,
	}
}

func buildCreateIndexAction(schemaDialect Dialect, index schemax.IndexMeta) schemax.MigrationAction {
	statement, err := schemaDialect.BuildCreateIndex(index)
	if err != nil {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   index.Table,
			Summary: "manual create index migration required for " + index.Name + ": " + err.Error(),
		}
	}
	return schemax.MigrationAction{
		Kind:       schemax.MigrationActionCreateIndex,
		Table:      index.Table,
		Summary:    "create index " + index.Name,
		Statement:  statement,
		Executable: true,
	}
}

func buildAddForeignKeyAction(schemaDialect Dialect, table string, foreignKey schemax.ForeignKeyMeta) schemax.MigrationAction {
	statement, err := schemaDialect.BuildAddForeignKey(table, foreignKey)
	if err != nil {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   table,
			Summary: "manual add foreign key migration required for " + foreignKey.Name + ": " + err.Error(),
		}
	}
	return schemax.MigrationAction{
		Kind:       schemax.MigrationActionAddForeignKey,
		Table:      table,
		Summary:    "add foreign key " + foreignKey.Name,
		Statement:  statement,
		Executable: true,
	}
}

func buildAddCheckAction(schemaDialect Dialect, table string, check schemax.CheckMeta) schemax.MigrationAction {
	statement, err := schemaDialect.BuildAddCheck(table, check)
	if err != nil {
		return schemax.MigrationAction{
			Kind:    schemax.MigrationActionManual,
			Table:   table,
			Summary: "manual add check migration required for " + check.Name + ": " + err.Error(),
		}
	}
	return schemax.MigrationAction{
		Kind:       schemax.MigrationActionAddCheck,
		Table:      table,
		Summary:    "add check " + check.Name,
		Statement:  statement,
		Executable: true,
	}
}

func primaryKeyIssues(expected *schemax.PrimaryKeyMeta, actual *schemax.PrimaryKeyState) []string {
	switch {
	case expected == nil && actual == nil:
		return nil
	case expected == nil:
		return []string{"unexpected primary key present"}
	case actual == nil:
		return []string{"missing primary key"}
	case columnsKey(expected.Columns) != columnsKey(actual.Columns):
		return []string{"primary key columns mismatch"}
	default:
		return nil
	}
}

func indexKey(unique bool, columns collectionx.List[string]) string {
	prefix := "idx:"
	if unique {
		prefix = "ux:"
	}
	return prefix + columnsKey(columns)
}

func foreignKeyKey(meta schemax.ForeignKeyMeta) string {
	return columnsKey(meta.Columns) + "->" + meta.TargetTable + ":" + columnsKey(meta.TargetColumns) + ":" + string(normalizeReferentialAction(meta.OnDelete)) + ":" + string(normalizeReferentialAction(meta.OnUpdate))
}

func foreignKeyKeyFromState(state schemax.ForeignKeyState) string {
	return columnsKey(state.Columns) + "->" + state.TargetTable + ":" + columnsKey(state.TargetColumns) + ":" + string(normalizeReferentialAction(state.OnDelete)) + ":" + string(normalizeReferentialAction(state.OnUpdate))
}

func checkKey(expression string) string {
	return normalizeCheckExpression(expression)
}

func columnsKey(columns collectionx.List[string]) string {
	return columns.Join(",")
}

func normalizeReferentialAction(action schemax.ReferentialAction) schemax.ReferentialAction {
	if strings.TrimSpace(string(action)) == "" {
		return schemax.ReferentialNoAction
	}
	return action
}

func typeMismatchIssue(expectedType, actualType string) string {
	if expectedType == "" || actualType == "" || expectedType == actualType {
		return ""
	}
	return "type mismatch: expected " + expectedType + " got " + actualType
}

func nullableMismatchIssue(expected schemax.ColumnMeta, actual schemax.ColumnState) string {
	if actual.PrimaryKey || expected.Nullable == actual.Nullable {
		return ""
	}
	return "nullable mismatch"
}

func autoIncrementMismatchIssue(expected schemax.ColumnMeta, actual schemax.ColumnState) string {
	if expected.AutoIncrement == actual.AutoIncrement {
		return ""
	}
	return "auto increment mismatch"
}

func defaultMismatchIssue(expected schemax.ColumnMeta, actual schemax.ColumnState) string {
	if expected.DefaultValue == "" || normalizeDefault(expected.DefaultValue) == normalizeDefault(actual.DefaultValue) {
		return ""
	}
	return "default mismatch"
}
