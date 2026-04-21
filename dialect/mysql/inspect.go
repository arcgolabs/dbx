package mysql

import (
	"context"
	"database/sql"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
)

const (
	mysqlTableExistsQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?"
	mysqlColumnsQuery     = "SELECT column_name, column_type, is_nullable, column_default, column_key, extra FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position"
	mysqlIndexesQuery     = "SELECT index_name, non_unique, column_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? ORDER BY index_name, seq_in_index"
	mysqlForeignKeysQuery = "SELECT kcu.constraint_name, kcu.column_name, kcu.referenced_table_name, kcu.referenced_column_name, rc.UPDATE_RULE, rc.DELETE_RULE FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name LEFT JOIN information_schema.referential_constraints rc ON kcu.constraint_name = rc.constraint_name AND kcu.table_schema = rc.constraint_schema WHERE kcu.table_schema = DATABASE() AND kcu.table_name = ? AND tc.constraint_type = 'FOREIGN KEY' ORDER BY kcu.constraint_name, kcu.ordinal_position"
	mysqlChecksQuery      = "SELECT tc.constraint_name, cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema WHERE tc.table_schema = DATABASE() AND tc.table_name = ? AND tc.constraint_type = 'CHECK' ORDER BY tc.constraint_name"
)

func inspectMySQLTableExists(ctx context.Context, executor dbx.Executor, table string) (exists bool, resultErr error) {
	const action = "inspect mysql table existence"

	rows, err := queryMySQLRows(ctx, executor, action, mysqlTableExistsQuery, table)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := closeMySQLRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	exists = rows.Next()
	if rowsErr := mysqlRowsError(action, rows); rowsErr != nil {
		return false, rowsErr
	}

	return exists, nil
}

func (d Dialect) inspectColumns(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.ColumnState, _ *schemax.PrimaryKeyState, resultErr error) {
	const action = "inspect mysql columns"

	rows, err := queryMySQLRows(ctx, executor, action, mysqlColumnsQuery, table)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := closeMySQLRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	columns := make([]schemax.ColumnState, 0, 8)
	primaryColumns := make([]string, 0, 2)
	for rows.Next() {
		column, isPrimary, scanErr := scanMySQLColumn(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		columns = append(columns, column)
		if isPrimary {
			primaryColumns = append(primaryColumns, column.Name)
		}
	}

	if rowsErr := mysqlRowsError(action, rows); rowsErr != nil {
		return nil, nil, rowsErr
	}

	return columns, mysqlPrimaryKeyState(primaryColumns), nil
}

func (d Dialect) inspectIndexes(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.IndexState, resultErr error) {
	const action = "inspect mysql indexes"

	rows, err := queryMySQLRows(ctx, executor, action, mysqlIndexesQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeMySQLRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	groups := collectionx.NewOrderedMap[string, schemax.IndexState]()
	for rows.Next() {
		name, state, scanErr := scanMySQLIndex(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		if strings.EqualFold(name, "PRIMARY") {
			continue
		}
		appendMySQLIndex(groups, name, state)
	}

	if rowsErr := mysqlRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	indexes := make([]schemax.IndexState, 0, groups.Len())
	groups.Range(func(_ string, value schemax.IndexState) bool {
		indexes = append(indexes, value)
		return true
	})
	return indexes, nil
}

func (d Dialect) inspectForeignKeys(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.ForeignKeyState, resultErr error) {
	const action = "inspect mysql foreign keys"

	rows, err := queryMySQLRows(ctx, executor, action, mysqlForeignKeysQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeMySQLRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	groups := collectionx.NewOrderedMap[string, schemax.ForeignKeyState]()
	for rows.Next() {
		name, state, scanErr := scanMySQLForeignKey(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		current, ok := groups.Get(name)
		if !ok {
			groups.Set(name, state)
			continue
		}

		current.Columns.Merge(state.Columns)
		current.TargetColumns.Merge(state.TargetColumns)
		groups.Set(name, current)
	}

	if rowsErr := mysqlRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	foreignKeys := make([]schemax.ForeignKeyState, 0, groups.Len())
	groups.Range(func(_ string, value schemax.ForeignKeyState) bool {
		foreignKeys = append(foreignKeys, value)
		return true
	})
	return foreignKeys, nil
}

func (d Dialect) inspectChecks(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.CheckState, resultErr error) {
	const action = "inspect mysql checks"

	rows, err := queryMySQLRows(ctx, executor, action, mysqlChecksQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeMySQLRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	checks := make([]schemax.CheckState, 0, 4)
	for rows.Next() {
		check, scanErr := scanMySQLCheck(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		checks = append(checks, check)
	}

	if rowsErr := mysqlRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return checks, nil
}

func scanMySQLColumn(rows *sql.Rows) (schemax.ColumnState, bool, error) {
	var name string
	var columnType string
	var isNullable string
	var columnKey string
	var extra string
	var defaultValue sql.NullString

	if err := rows.Scan(&name, &columnType, &isNullable, &defaultValue, &columnKey, &extra); err != nil {
		return schemax.ColumnState{}, false, fmt.Errorf("scan mysql column: %w", err)
	}

	isPrimary := strings.EqualFold(columnKey, "PRI")
	return schemax.ColumnState{
		Name:          name,
		Type:          columnType,
		Nullable:      strings.EqualFold(isNullable, "YES"),
		PrimaryKey:    isPrimary,
		AutoIncrement: strings.Contains(strings.ToLower(extra), "auto_increment"),
		DefaultValue:  defaultValue.String,
	}, isPrimary, nil
}

func scanMySQLIndex(rows *sql.Rows) (string, schemax.IndexState, error) {
	var name string
	var column string
	var nonUnique int

	if err := rows.Scan(&name, &nonUnique, &column); err != nil {
		return "", schemax.IndexState{}, fmt.Errorf("scan mysql index: %w", err)
	}

	return name, schemax.IndexState{
		Name:    name,
		Columns: collectionx.NewList[string](column),
		Unique:  nonUnique == 0,
	}, nil
}

func appendMySQLIndex(groups collectionx.OrderedMap[string, schemax.IndexState], name string, state schemax.IndexState) {
	current, ok := groups.Get(name)
	if !ok {
		groups.Set(name, state)
		return
	}
	current.Columns.Merge(state.Columns)
	groups.Set(name, current)
}

func scanMySQLForeignKey(rows *sql.Rows) (string, schemax.ForeignKeyState, error) {
	var name string
	var column string
	var targetTable string
	var targetColumn string
	var updateRule sql.NullString
	var deleteRule sql.NullString

	if err := rows.Scan(&name, &column, &targetTable, &targetColumn, &updateRule, &deleteRule); err != nil {
		return "", schemax.ForeignKeyState{}, fmt.Errorf("scan mysql foreign key: %w", err)
	}

	return name, schemax.ForeignKeyState{
		Name:          name,
		TargetTable:   targetTable,
		Columns:       collectionx.NewList[string](column),
		TargetColumns: collectionx.NewList[string](targetColumn),
		OnDelete:      referentialAction(deleteRule.String),
		OnUpdate:      referentialAction(updateRule.String),
	}, nil
}

func scanMySQLCheck(rows *sql.Rows) (schemax.CheckState, error) {
	var name string
	var clause string

	if err := rows.Scan(&name, &clause); err != nil {
		return schemax.CheckState{}, fmt.Errorf("scan mysql check: %w", err)
	}

	return schemax.CheckState{Name: name, Expression: clause}, nil
}

func mysqlPrimaryKeyState(columns []string) *schemax.PrimaryKeyState {
	if len(columns) == 0 {
		return nil
	}
	return &schemax.PrimaryKeyState{Name: "PRIMARY", Columns: collectionx.NewList[string](columns...)}
}

func queryMySQLRows(ctx context.Context, executor dbx.Executor, action, query string, args ...any) (*sql.Rows, error) {
	rows, err := executor.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", action, err)
	}
	return rows, nil
}

func closeMySQLRows(action string, rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	if closeErr := rows.Close(); closeErr != nil {
		return fmt.Errorf("%s: close rows: %w", action, closeErr)
	}
	return nil
}

func mysqlRowsError(action string, rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: rows err: %w", action, err)
	}
	return nil
}
