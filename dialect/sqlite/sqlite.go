package sqlite

import (
	"context"
	"errors"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
)

var (
	sqliteIntegerKinds = []reflect.Kind{
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
	}
	sqliteRealKinds = []reflect.Kind{reflect.Float32, reflect.Float64}
)

// Dialect implements SQLite rendering and schema inspection.
type Dialect struct{}

// New returns a SQLite dialect implementation.
func New() Dialect { return Dialect{} }

// Name returns the dialect name.
func (Dialect) Name() string { return "sqlite" }

// BindVar returns the bind placeholder for a parameter index.
func (Dialect) BindVar(_ int) string { return "?" }

// QuoteIdent quotes an identifier for SQLite.
func (Dialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// RenderLimitOffset renders a LIMIT/OFFSET clause for SQLite.
func (Dialect) RenderLimitOffset(limit, offset *int) (string, error) {
	if limit == nil && offset == nil {
		return "", nil
	}
	if limit != nil && offset != nil {
		return fmt.Sprintf("LIMIT %d OFFSET %d", *limit, *offset), nil
	}
	if limit != nil {
		return fmt.Sprintf("LIMIT %d", *limit), nil
	}
	return fmt.Sprintf("LIMIT -1 OFFSET %d", *offset), nil
}

// QueryFeatures returns the supported query feature set.
func (Dialect) QueryFeatures() dialect.QueryFeatures {
	return dialect.DefaultQueryFeatures("sqlite")
}

// BuildCreateTable builds a CREATE TABLE statement.
func (d Dialect) BuildCreateTable(spec schemax.TableSpec) (sqlstmt.Bound, error) {
	parts := collectionx.NewListWithCapacity[string](spec.Columns.Len() + spec.ForeignKeys.Len() + spec.Checks.Len() + 1)
	inlinePrimaryKey := singlePrimaryKeyColumn(spec.PrimaryKey)

	var buildErr error
	spec.Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		ddl, err := d.columnDDL(column, columnDDLConfig{
			AllowAutoIncrement: true,
			InlinePrimaryKey:   inlinePrimaryKey == column.Name,
		})
		if err != nil {
			buildErr = fmt.Errorf("build sqlite column ddl: %w", err)
			return false
		}
		parts.Add(ddl)
		return true
	})
	if buildErr != nil {
		return sqlstmt.Bound{}, buildErr
	}

	if spec.PrimaryKey != nil && spec.PrimaryKey.Columns.Len() > 1 {
		parts.Add(d.primaryKeyDDL(*spec.PrimaryKey))
	}

	spec.ForeignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		parts.Add(d.foreignKeyDDL(foreignKey))
		return true
	})
	spec.Checks.Range(func(_ int, check schemax.CheckMeta) bool {
		parts.Add(d.checkDDL(check))
		return true
	})

	return sqlstmt.Bound{
		SQL: "CREATE TABLE IF NOT EXISTS " + d.QuoteIdent(spec.Name) + " (" + joinSQLiteStrings(parts, ", ") + ")",
	}, nil
}

// BuildAddColumn builds an ALTER TABLE ADD COLUMN statement.
func (d Dialect) BuildAddColumn(table string, column schemax.ColumnMeta) (sqlstmt.Bound, error) {
	if column.PrimaryKey {
		return sqlstmt.Bound{}, fmt.Errorf("dbx/sqlite: cannot add primary key column %s with ALTER TABLE", column.Name)
	}

	ddl, err := d.columnDDL(column, columnDDLConfig{IncludeReference: true})
	if err != nil {
		return sqlstmt.Bound{}, fmt.Errorf("build sqlite column ddl: %w", err)
	}

	return sqlstmt.Bound{
		SQL: "ALTER TABLE " + d.QuoteIdent(table) + " ADD COLUMN " + ddl,
	}, nil
}

// BuildCreateIndex builds a CREATE INDEX statement.
func (d Dialect) BuildCreateIndex(index schemax.IndexMeta) (sqlstmt.Bound, error) {
	prefix := "CREATE INDEX IF NOT EXISTS "
	if index.Unique {
		prefix = "CREATE UNIQUE INDEX IF NOT EXISTS "
	}
	return sqlstmt.Bound{
		SQL: prefix + d.QuoteIdent(index.Name) + " ON " + d.QuoteIdent(index.Table) + " (" + d.joinQuotedIdentifiers(index.Columns) + ")",
	}, nil
}

// BuildAddForeignKey reports that SQLite foreign keys require a table rebuild.
func (Dialect) BuildAddForeignKey(string, schemax.ForeignKeyMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{}, errors.New("dbx/sqlite: adding foreign keys requires table rebuild")
}

// BuildAddCheck reports that SQLite check constraints require a table rebuild.
func (Dialect) BuildAddCheck(string, schemax.CheckMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{}, errors.New("dbx/sqlite: adding check constraints requires table rebuild")
}

// InspectTable inspects a SQLite table definition from PRAGMA metadata.
func (d Dialect) InspectTable(ctx context.Context, executor dbx.Executor, table string) (schemax.TableState, error) {
	exists, err := inspectSQLiteTableExists(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}
	if !exists {
		return schemax.TableState{Name: table, Exists: false}, nil
	}

	columns, primaryKey, err := d.inspectColumns(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}

	indexes, err := d.inspectIndexes(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}

	foreignKeys, err := d.inspectForeignKeys(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}

	checks, autoincrementColumns, err := inspectSQLiteCreateMetadata(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}

	return schemax.TableState{
		Exists:      true,
		Name:        table,
		Columns:     collectionx.NewListWithCapacity[schemax.ColumnState](len(columns), markSQLiteAutoincrementColumns(columns, autoincrementColumns)...),
		Indexes:     collectionx.NewListWithCapacity[schemax.IndexState](len(indexes), indexes...),
		PrimaryKey:  primaryKey,
		ForeignKeys: collectionx.NewListWithCapacity[schemax.ForeignKeyState](len(foreignKeys), foreignKeys...),
		Checks:      collectionx.NewListWithCapacity[schemax.CheckState](len(checks), checks...),
	}, nil
}

// NormalizeType normalizes database type names into dbx logical types.
func (Dialect) NormalizeType(value string) string {
	typeName := strings.ToUpper(strings.TrimSpace(value))
	switch {
	case strings.Contains(typeName, "INT"):
		return "INTEGER"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "TEXT"):
		return "TEXT"
	case strings.Contains(typeName, "BLOB"):
		return "BLOB"
	case strings.Contains(typeName, "REAL"), strings.Contains(typeName, "FLOA"), strings.Contains(typeName, "DOUBLE"):
		return "REAL"
	case strings.Contains(typeName, "BOOL"):
		return "BOOLEAN"
	case strings.Contains(typeName, "TIMESTAMP"), strings.Contains(typeName, "DATETIME"):
		return "TIMESTAMP"
	default:
		return typeName
	}
}

var _ schemamigrate.Dialect = Dialect{}
