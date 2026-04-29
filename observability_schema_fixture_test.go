package dbx_test

import (
	"context"
	"fmt"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
)

type fakeSchemaDialect struct {
	tables map[string]schemax.TableState
}

func newFakeSchemaDialect() *fakeSchemaDialect {
	return &fakeSchemaDialect{tables: make(map[string]schemax.TableState)}
}

func (d *fakeSchemaDialect) Name() string         { return "fake" }
func (d *fakeSchemaDialect) BindVar(_ int) string { return "?" }
func (d *fakeSchemaDialect) QuoteIdent(ident string) string {
	return `"` + ident + `"`
}
func (d *fakeSchemaDialect) RenderLimitOffset(limit, offset *int) (string, error) {
	return testSQLiteDialect{}.RenderLimitOffset(limit, offset)
}
func (d *fakeSchemaDialect) NormalizeType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
func (d *fakeSchemaDialect) BuildCreateTable(spec schemax.TableSpec) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: "create table " + spec.Name}, nil
}
func (d *fakeSchemaDialect) BuildAddColumn(table string, column schemax.ColumnMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: fmt.Sprintf("alter table %s add column %s", table, column.Name)}, nil
}
func (d *fakeSchemaDialect) BuildCreateIndex(index schemax.IndexMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: "create index " + index.Name}, nil
}
func (d *fakeSchemaDialect) BuildAddForeignKey(table string, foreignKey schemax.ForeignKeyMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: "alter table " + table + " add constraint " + foreignKey.Name}, nil
}
func (d *fakeSchemaDialect) BuildAddCheck(table string, check schemax.CheckMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: "alter table " + table + " add constraint " + check.Name}, nil
}
func (d *fakeSchemaDialect) InspectTable(_ context.Context, _ Executor, table string) (schemax.TableState, error) {
	if state, ok := d.tables[table]; ok {
		copyState := state
		copyState.Columns = state.Columns.Clone()
		copyState.Indexes = state.Indexes.Clone()
		if state.PrimaryKey != nil {
			copyState.PrimaryKey = &schemax.PrimaryKeyState{Name: state.PrimaryKey.Name, Columns: state.PrimaryKey.Columns.Clone()}
		}
		copyState.ForeignKeys = state.ForeignKeys.Clone()
		copyState.Checks = state.Checks.Clone()
		return copyState, nil
	}
	return schemax.TableState{Name: table, Exists: false}, nil
}

func toColumnState(column schemax.ColumnMeta) schemax.ColumnState {
	typeName := column.SQLType
	if typeName == "" {
		typeName = InferTypeNameForTest(column)
	}
	return schemax.ColumnState{
		Name:          column.Name,
		Type:          strings.ToLower(typeName),
		Nullable:      column.Nullable,
		PrimaryKey:    column.PrimaryKey,
		AutoIncrement: column.AutoIncrement,
		DefaultValue:  column.DefaultValue,
	}
}

func toIndexStates(indexes *collectionx.List[schemax.IndexMeta]) *collectionx.List[schemax.IndexState] {
	items := collectionx.NewListWithCapacity[schemax.IndexState](indexes.Len())
	indexes.Range(func(_ int, index schemax.IndexMeta) bool {
		items.Add(schemax.IndexState{Name: index.Name, Columns: index.Columns.Clone(), Unique: index.Unique})
		return true
	})
	return items
}

func toForeignKeyStates(foreignKeys *collectionx.List[schemax.ForeignKeyMeta]) *collectionx.List[schemax.ForeignKeyState] {
	items := collectionx.NewListWithCapacity[schemax.ForeignKeyState](foreignKeys.Len())
	foreignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		items.Add(schemax.ForeignKeyState{
			Name:          foreignKey.Name,
			Columns:       foreignKey.Columns.Clone(),
			TargetTable:   foreignKey.TargetTable,
			TargetColumns: foreignKey.TargetColumns.Clone(),
			OnDelete:      foreignKey.OnDelete,
			OnUpdate:      foreignKey.OnUpdate,
		})
		return true
	})
	return items
}

var _ interface {
	InspectTable(context.Context, Executor, string) (schemax.TableState, error)
} = (*fakeSchemaDialect)(nil)
