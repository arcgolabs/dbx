package sqlite

import (
	"context"
	"database/sql"
	schemax "github.com/arcgolabs/dbx/schema"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
)

const (
	sqliteTableExistsQuery = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?"
	sqliteCreateSQLQuery   = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?"
)

func inspectSQLiteTableExists(ctx context.Context, executor dbx.Executor, table string) (exists bool, resultErr error) {
	const action = "inspect sqlite table existence"

	rows, err := querySQLiteRows(ctx, executor, action, sqliteTableExistsQuery, table)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	exists = rows.Next()
	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return false, rowsErr
	}

	return exists, nil
}

func (d Dialect) inspectColumns(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.ColumnState, _ *schemax.PrimaryKeyState, resultErr error) {
	const action = "inspect sqlite columns"

	rows, err := querySQLiteRows(ctx, executor, action, "PRAGMA table_info("+d.QuoteIdent(table)+")")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	columns := make([]schemax.ColumnState, 0, 8)
	primaryPositions := make(map[int]string, 2)
	for rows.Next() {
		column, primaryPosition, scanErr := scanSQLiteColumn(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		columns = append(columns, column)
		if primaryPosition > 0 {
			primaryPositions[primaryPosition] = column.Name
		}
	}

	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return nil, nil, rowsErr
	}

	return columns, sqlitePrimaryKeyState(primaryPositions), nil
}

func (d Dialect) inspectIndexes(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.IndexState, resultErr error) {
	const action = "inspect sqlite indexes"

	rows, err := querySQLiteRows(ctx, executor, action, "PRAGMA index_list("+d.QuoteIdent(table)+")")
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	indexes := make([]schemax.IndexState, 0, 4)
	for rows.Next() {
		index, skip, indexErr := d.loadSQLiteIndex(ctx, executor, rows)
		if indexErr != nil {
			return nil, indexErr
		}
		if !skip {
			indexes = append(indexes, index)
		}
	}

	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return indexes, nil
}

func (d Dialect) loadSQLiteIndex(ctx context.Context, executor dbx.Executor, rows *sql.Rows) (schemax.IndexState, bool, error) {
	name, unique, origin, err := scanSQLiteIndexList(rows)
	if err != nil {
		return schemax.IndexState{}, false, err
	}
	if origin == "pk" {
		return schemax.IndexState{}, true, nil
	}

	index, err := d.inspectIndex(ctx, executor, name, unique)
	if err != nil {
		return schemax.IndexState{}, false, err
	}
	return index, false, nil
}

func (d Dialect) inspectIndex(ctx context.Context, executor dbx.Executor, name string, unique bool) (schemax.IndexState, error) {
	columns, err := d.inspectIndexColumns(ctx, executor, name)
	if err != nil {
		return schemax.IndexState{}, err
	}
	return schemax.IndexState{Name: name, Columns: collectionx.NewList[string](columns...), Unique: unique}, nil
}

func (d Dialect) inspectIndexColumns(ctx context.Context, executor dbx.Executor, name string) (_ []string, resultErr error) {
	const action = "inspect sqlite index columns"

	rows, err := querySQLiteRows(ctx, executor, action, "PRAGMA index_info("+d.QuoteIdent(name)+")")
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	columns := make([]string, 0, 2)
	for rows.Next() {
		column, scanErr := scanSQLiteIndexColumn(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		columns = append(columns, column)
	}

	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return columns, nil
}

func (d Dialect) inspectForeignKeys(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.ForeignKeyState, resultErr error) {
	const action = "inspect sqlite foreign keys"

	rows, err := querySQLiteRows(ctx, executor, action, "PRAGMA foreign_key_list("+d.QuoteIdent(table)+")")
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	groups := collectionx.NewOrderedMap[int, schemax.ForeignKeyState]()
	for rows.Next() {
		id, state, scanErr := scanSQLiteForeignKey(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		appendSQLiteForeignKey(groups, id, state)
	}

	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	foreignKeys := make([]schemax.ForeignKeyState, 0, groups.Len())
	groups.Range(func(_ int, value schemax.ForeignKeyState) bool {
		foreignKeys = append(foreignKeys, value)
		return true
	})
	return foreignKeys, nil
}

func inspectSQLiteCreateMetadata(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.CheckState, _ map[string]struct{}, resultErr error) {
	const action = "inspect sqlite create metadata"

	rows, err := querySQLiteRows(ctx, executor, action, sqliteCreateSQLQuery, table)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := closeSQLiteRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	checks := make([]schemax.CheckState, 0, 2)
	autoincrementColumns := make(map[string]struct{}, 1)
	for rows.Next() {
		createSQL, scanErr := scanSQLiteCreateSQL(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}

		cols := parseCreateTableAutoincrementColumns(createSQL)
		for i := range cols {
			column := cols[i]
			autoincrementColumns[column] = struct{}{}
		}
		checks = append(checks, parseCreateTableChecks(createSQL)...)
	}

	if rowsErr := sqliteRowsError(action, rows); rowsErr != nil {
		return nil, nil, rowsErr
	}

	return checks, autoincrementColumns, nil
}
