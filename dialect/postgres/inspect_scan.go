package postgres

import (
	"context"
	"database/sql"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx"
)

func scanPostgresPrimaryKey(rows *sql.Rows) (string, string, error) {
	var name string
	var column string

	if err := rows.Scan(&name, &column); err != nil {
		return "", "", fmt.Errorf("scan postgres primary key: %w", err)
	}
	return name, column, nil
}

func scanPostgresColumn(rows *sql.Rows) (schemax.ColumnState, error) {
	var name string
	var udtName string
	var isNullable string
	var defaultValue sql.NullString
	var isIdentity bool

	if err := rows.Scan(&name, &udtName, &isNullable, &defaultValue, &isIdentity); err != nil {
		return schemax.ColumnState{}, fmt.Errorf("scan postgres column: %w", err)
	}

	return schemax.ColumnState{
		Name:          name,
		Type:          udtName,
		Nullable:      strings.EqualFold(isNullable, "YES"),
		AutoIncrement: isIdentity || strings.Contains(strings.ToLower(defaultValue.String), "nextval"),
		DefaultValue:  defaultValue.String,
	}, nil
}

func scanPostgresIndex(rows *sql.Rows) (schemax.IndexState, bool, error) {
	var name string
	var definition string

	if err := rows.Scan(&name, &definition); err != nil {
		return schemax.IndexState{}, false, fmt.Errorf("scan postgres index: %w", err)
	}

	upperDefinition := strings.ToUpper(definition)
	if strings.Contains(upperDefinition, "PRIMARY KEY") {
		return schemax.IndexState{}, true, nil
	}

	return schemax.IndexState{
		Name:    name,
		Columns: collectionx.NewList[string](parseIndexColumns(definition)...),
		Unique:  strings.Contains(upperDefinition, "CREATE UNIQUE INDEX"),
	}, false, nil
}

func scanPostgresForeignKey(rows *sql.Rows) (string, schemax.ForeignKeyState, error) {
	var name string
	var column string
	var targetTable string
	var targetColumn string
	var updateRule string
	var deleteRule string

	if err := rows.Scan(&name, &column, &targetTable, &targetColumn, &updateRule, &deleteRule); err != nil {
		return "", schemax.ForeignKeyState{}, fmt.Errorf("scan postgres foreign key: %w", err)
	}

	return name, schemax.ForeignKeyState{
		Name:          name,
		TargetTable:   targetTable,
		Columns:       collectionx.NewList[string](column),
		TargetColumns: collectionx.NewList[string](targetColumn),
		OnDelete:      referentialAction(deleteRule),
		OnUpdate:      referentialAction(updateRule),
	}, nil
}

func scanPostgresCheck(rows *sql.Rows) (schemax.CheckState, error) {
	var name string
	var clause string

	if err := rows.Scan(&name, &clause); err != nil {
		return schemax.CheckState{}, fmt.Errorf("scan postgres check: %w", err)
	}

	return schemax.CheckState{Name: name, Expression: clause}, nil
}

func postgresPrimaryKeyState(name string, columns []string) *schemax.PrimaryKeyState {
	if len(columns) == 0 {
		return nil
	}
	return &schemax.PrimaryKeyState{Name: name, Columns: collectionx.NewList[string](columns...)}
}

func postgresPrimaryColumn(columns map[string]struct{}, name string) bool {
	_, ok := columns[name]
	return ok
}

func appendPostgresForeignKey(groups *mappingx.OrderedMap[string, schemax.ForeignKeyState], name string, state schemax.ForeignKeyState) {
	current, ok := groups.Get(name)
	if !ok {
		groups.Set(name, state)
		return
	}
	current.Columns.Merge(state.Columns)
	current.TargetColumns.Merge(state.TargetColumns)
	groups.Set(name, current)
}

func queryPostgresRows(ctx context.Context, executor dbx.Executor, action, query string, args ...any) (*sql.Rows, error) {
	rows, err := executor.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", action, err)
	}
	return rows, nil
}

func closePostgresRows(action string, rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	if closeErr := rows.Close(); closeErr != nil {
		return fmt.Errorf("%s: close rows: %w", action, closeErr)
	}
	return nil
}

func postgresRowsError(action string, rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: rows err: %w", action, err)
	}
	return nil
}
