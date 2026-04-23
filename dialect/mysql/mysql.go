package mysql

import (
	"context"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
)

// Dialect implements MySQL rendering and schema inspection.
type Dialect struct{}

// New returns a MySQL dialect implementation.
func New() Dialect { return Dialect{} }

// Name returns the dialect name.
func (Dialect) Name() string { return "mysql" }

// BindVar returns the bind placeholder for a parameter index.
func (Dialect) BindVar(_ int) string { return "?" }

// QuoteIdent quotes an identifier for MySQL.
func (Dialect) QuoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// RenderLimitOffset renders a LIMIT/OFFSET clause for MySQL.
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
	return fmt.Sprintf("LIMIT 18446744073709551615 OFFSET %d", *offset), nil
}

// QueryFeatures returns the supported query feature set.
func (Dialect) QueryFeatures() dialect.QueryFeatures {
	return dialect.DefaultQueryFeatures("mysql")
}

// BuildCreateTable builds a CREATE TABLE statement.
func (d Dialect) BuildCreateTable(spec schemax.TableSpec) (sqlstmt.Bound, error) {
	parts := collectionx.NewListWithCapacity[string](spec.Columns.Len() + spec.ForeignKeys.Len() + spec.Checks.Len() + 1)
	inlinePrimaryKey := singlePrimaryKeyColumn(spec.PrimaryKey)
	spec.Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		parts.Add(d.columnDDL(column, inlinePrimaryKey == column.Name, false))
		return true
	})
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
		SQL: "CREATE TABLE IF NOT EXISTS " + d.QuoteIdent(spec.Name) + " (" + joinMySQLStrings(parts, ", ") + ")",
	}, nil
}

// BuildAddColumn builds an ALTER TABLE ADD COLUMN statement.
func (d Dialect) BuildAddColumn(table string, column schemax.ColumnMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{
		SQL: "ALTER TABLE " + d.QuoteIdent(table) + " ADD COLUMN " + d.columnDDL(column, false, true),
	}, nil
}

// BuildCreateIndex builds a CREATE INDEX statement.
func (d Dialect) BuildCreateIndex(index schemax.IndexMeta) (sqlstmt.Bound, error) {
	prefix := "CREATE INDEX "
	if index.Unique {
		prefix = "CREATE UNIQUE INDEX "
	}
	return sqlstmt.Bound{
		SQL: prefix + d.QuoteIdent(index.Name) + " ON " + d.QuoteIdent(index.Table) + " (" + d.joinQuotedIdentifiers(index.Columns) + ")",
	}, nil
}

// BuildAddForeignKey builds an ALTER TABLE ADD CONSTRAINT statement for a foreign key.
func (d Dialect) BuildAddForeignKey(table string, foreignKey schemax.ForeignKeyMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{
		SQL: "ALTER TABLE " + d.QuoteIdent(table) + " ADD " + d.foreignKeyDDL(foreignKey),
	}, nil
}

// BuildAddCheck builds an ALTER TABLE ADD CONSTRAINT statement for a check.
func (d Dialect) BuildAddCheck(table string, check schemax.CheckMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{
		SQL: "ALTER TABLE " + d.QuoteIdent(table) + " ADD " + d.checkDDL(check),
	}, nil
}

// InspectTable inspects a MySQL table definition from information_schema.
func (d Dialect) InspectTable(ctx context.Context, executor dbx.Executor, table string) (schemax.TableState, error) {
	exists, err := inspectMySQLTableExists(ctx, executor, table)
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

	checks, err := d.inspectChecks(ctx, executor, table)
	if err != nil {
		return schemax.TableState{}, err
	}

	return schemax.TableState{
		Exists:      true,
		Name:        table,
		Columns:     collectionx.NewListWithCapacity[schemax.ColumnState](len(columns), columns...),
		Indexes:     collectionx.NewListWithCapacity[schemax.IndexState](len(indexes), indexes...),
		PrimaryKey:  primaryKey,
		ForeignKeys: collectionx.NewListWithCapacity[schemax.ForeignKeyState](len(foreignKeys), foreignKeys...),
		Checks:      collectionx.NewListWithCapacity[schemax.CheckState](len(checks), checks...),
	}, nil
}

// NormalizeType normalizes database type names into dbx logical types.
func (Dialect) NormalizeType(value string) string {
	typeName := mysqlNormalizedTypeName(value)
	if normalized, ok := mysqlNormalizedTypes[typeName]; ok {
		return normalized
	}
	return typeName
}

var _ schemamigrate.Dialect = Dialect{}
