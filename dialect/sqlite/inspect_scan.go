package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"maps"
	"slices"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx"
)

func scanSQLiteColumn(rows *sql.Rows) (schemax.ColumnState, int, error) {
	var cid int
	var name string
	var typeName string
	var notNull int
	var defaultValue sql.NullString
	var primaryPosition int

	if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryPosition); err != nil {
		return schemax.ColumnState{}, 0, fmt.Errorf("scan sqlite column: %w", err)
	}

	return schemax.ColumnState{
		Name:         name,
		Type:         typeName,
		Nullable:     notNull == 0,
		PrimaryKey:   primaryPosition > 0,
		DefaultValue: defaultValue.String,
	}, primaryPosition, nil
}

func scanSQLiteIndexList(rows *sql.Rows) (string, bool, string, error) {
	var seq int
	var name string
	var unique int
	var origin string
	var partial int

	if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
		return "", false, "", fmt.Errorf("scan sqlite index list: %w", err)
	}

	return name, unique == 1, origin, nil
}

func scanSQLiteIndexColumn(rows *sql.Rows) (string, error) {
	var seqno int
	var cid int
	var column string

	if err := rows.Scan(&seqno, &cid, &column); err != nil {
		return "", fmt.Errorf("scan sqlite index column: %w", err)
	}

	return column, nil
}

func scanSQLiteForeignKey(rows *sql.Rows) (int, schemax.ForeignKeyState, error) {
	var id int
	var seq int
	var targetTable string
	var from string
	var to string
	var onUpdate string
	var onDelete string
	var match string

	if err := rows.Scan(&id, &seq, &targetTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
		return 0, schemax.ForeignKeyState{}, fmt.Errorf("scan sqlite foreign key: %w", err)
	}

	return id, schemax.ForeignKeyState{
		TargetTable:   targetTable,
		Columns:       collectionx.NewList[string](from),
		TargetColumns: collectionx.NewList[string](to),
		OnDelete:      referentialAction(onDelete),
		OnUpdate:      referentialAction(onUpdate),
	}, nil
}

func scanSQLiteCreateSQL(rows *sql.Rows) (string, error) {
	var createSQL sql.NullString

	if err := rows.Scan(&createSQL); err != nil {
		return "", fmt.Errorf("scan sqlite create sql: %w", err)
	}
	return createSQL.String, nil
}

func sqlitePrimaryKeyState(positions map[int]string) *schemax.PrimaryKeyState {
	if len(positions) == 0 {
		return nil
	}

	keys := slices.Sorted(maps.Keys(positions))
	columns := collectionx.MapList[int, string](collectionx.NewList[int](keys...), func(_ int, position int) string {
		return positions[position]
	})
	return &schemax.PrimaryKeyState{Columns: columns}
}

func appendSQLiteForeignKey(groups *mappingx.OrderedMap[int, schemax.ForeignKeyState], id int, state schemax.ForeignKeyState) {
	current, ok := groups.Get(id)
	if !ok {
		groups.Set(id, state)
		return
	}
	current.Columns.Merge(state.Columns)
	current.TargetColumns.Merge(state.TargetColumns)
	groups.Set(id, current)
}

func querySQLiteRows(ctx context.Context, executor dbx.Executor, action, query string, args ...any) (*sql.Rows, error) {
	rows, err := executor.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", action, err)
	}
	return rows, nil
}

func closeSQLiteRows(action string, rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	if closeErr := rows.Close(); closeErr != nil {
		return fmt.Errorf("%s: close rows: %w", action, closeErr)
	}
	return nil
}

func sqliteRowsError(action string, rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: rows err: %w", action, err)
	}
	return nil
}

func markSQLiteAutoincrementColumns(columns []schemax.ColumnState, autoincrementColumns map[string]struct{}) []schemax.ColumnState {
	items := collectionx.NewList[schemax.ColumnState](columns...)
	items.SetAllIndexed(func(_ int, column schemax.ColumnState) schemax.ColumnState {
		if _, ok := autoincrementColumns[column.Name]; ok {
			column.AutoIncrement = true
		}
		return column
	})
	return items.Values()
}
