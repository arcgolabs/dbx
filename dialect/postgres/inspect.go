package postgres

import (
	"context"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
)

const (
	postgresTableExistsQuery = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)"
	postgresPrimaryKeyQuery  = "SELECT tc.constraint_name, kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name WHERE tc.table_schema = current_schema() AND tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position"
	postgresColumnsQuery     = "SELECT c.column_name, c.udt_name, c.is_nullable, c.column_default, (c.is_identity = 'YES') AS is_identity FROM information_schema.columns c WHERE c.table_schema = current_schema() AND c.table_name = $1 ORDER BY c.ordinal_position"
	postgresIndexesQuery     = "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = $1"
	postgresForeignKeysQuery = "SELECT tc.constraint_name, kcu.column_name, ccu.table_name, ccu.column_name, rc.update_rule, rc.delete_rule FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema WHERE tc.table_schema = current_schema() AND tc.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY' ORDER BY tc.constraint_name, kcu.ordinal_position"
	postgresChecksQuery      = "SELECT tc.constraint_name, cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name AND tc.table_schema = cc.constraint_schema WHERE tc.table_schema = current_schema() AND tc.table_name = $1 AND tc.constraint_type = 'CHECK' ORDER BY tc.constraint_name"
)

func inspectPostgresTableExists(ctx context.Context, executor dbx.Executor, table string) (exists bool, resultErr error) {
	const action = "inspect postgres table existence"

	rows, err := queryPostgresRows(ctx, executor, action, postgresTableExistsQuery, table)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	if rows.Next() {
		if scanErr := rows.Scan(&exists); scanErr != nil {
			return false, fmt.Errorf("%s: scan row: %w", action, scanErr)
		}
	}
	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
		return false, rowsErr
	}

	return exists, nil
}

func inspectPostgresPrimaryKey(ctx context.Context, executor dbx.Executor, table string) (_ *schemax.PrimaryKeyState, _ map[string]struct{}, resultErr error) {
	const action = "inspect postgres primary key"

	rows, err := queryPostgresRows(ctx, executor, action, postgresPrimaryKeyQuery, table)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	columns := make([]string, 0, 2)
	columnSet := make(map[string]struct{}, 2)
	name := ""
	for rows.Next() {
		constraintName, column, scanErr := scanPostgresPrimaryKey(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		name = constraintName
		columns = append(columns, column)
		columnSet[column] = struct{}{}
	}

	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
		return nil, nil, rowsErr
	}

	return postgresPrimaryKeyState(name, columns), columnSet, nil
}

func (d Dialect) inspectColumns(ctx context.Context, executor dbx.Executor, table string, primaryColumns map[string]struct{}) (_ []schemax.ColumnState, resultErr error) {
	const action = "inspect postgres columns"

	rows, err := queryPostgresRows(ctx, executor, action, postgresColumnsQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	columns := make([]schemax.ColumnState, 0, 8)
	for rows.Next() {
		column, scanErr := scanPostgresColumn(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		column.PrimaryKey = postgresPrimaryColumn(primaryColumns, column.Name)
		columns = append(columns, column)
	}

	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return columns, nil
}

func (d Dialect) inspectIndexes(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.IndexState, resultErr error) {
	const action = "inspect postgres indexes"

	rows, err := queryPostgresRows(ctx, executor, action, postgresIndexesQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	indexes := make([]schemax.IndexState, 0, 4)
	for rows.Next() {
		index, skip, scanErr := scanPostgresIndex(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		if skip {
			continue
		}
		indexes = append(indexes, index)
	}

	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return indexes, nil
}

func (d Dialect) inspectForeignKeys(ctx context.Context, executor dbx.Executor, table string) (_ []schemax.ForeignKeyState, resultErr error) {
	const action = "inspect postgres foreign keys"

	rows, err := queryPostgresRows(ctx, executor, action, postgresForeignKeysQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	groups := collectionx.NewOrderedMap[string, schemax.ForeignKeyState]()
	for rows.Next() {
		name, state, scanErr := scanPostgresForeignKey(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		appendPostgresForeignKey(groups, name, state)
	}

	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
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
	const action = "inspect postgres checks"

	rows, err := queryPostgresRows(ctx, executor, action, postgresChecksQuery, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := closePostgresRows(action, rows); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()

	checks := make([]schemax.CheckState, 0, 4)
	for rows.Next() {
		check, scanErr := scanPostgresCheck(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		checks = append(checks, check)
	}

	if rowsErr := postgresRowsError(action, rows); rowsErr != nil {
		return nil, rowsErr
	}

	return checks, nil
}
