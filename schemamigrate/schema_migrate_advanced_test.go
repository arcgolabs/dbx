package schemamigrate_test

import (
	"context"
	"errors"
	"fmt"
	relationx "github.com/arcgolabs/dbx/relation"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
	"strings"
	"testing"

	"github.com/DaiYuANg/arcgo/collectionx"
)

type advancedRole struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type advancedUser struct {
	ID       int64  `dbx:"id"`
	TenantID int64  `dbx:"tenant_id"`
	Username string `dbx:"username"`
	Status   int    `dbx:"status"`
	RoleID   int64  `dbx:"role_id"`
}

type advancedRoleSchema struct {
	Schema[advancedRole]
	ID   Column[advancedRole, int64]  `dbx:"id,pk,auto"`
	Name Column[advancedRole, string] `dbx:"name,unique"`
}

type advancedUserSchema struct {
	Schema[advancedUser]
	ID          Column[advancedUser, int64]                     `dbx:"id"`
	TenantID    Column[advancedUser, int64]                     `dbx:"tenant_id"`
	Username    Column[advancedUser, string]                    `dbx:"username"`
	Status      Column[advancedUser, int]                       `dbx:"status"`
	RoleID      Column[advancedUser, int64]                     `dbx:"role_id"`
	Role        relationx.BelongsTo[advancedUser, advancedRole] `rel:"table=roles,local=role_id,target=id"`
	LookupIndex Index[advancedUser]                             `idx:"columns=tenant_id|username"`
	PK          CompositeKey[advancedUser]                      `key:"columns=id|tenant_id"`
	StatusCheck Check[advancedUser]                             `check:"expr=status >= 0"`
}

func TestPlanSchemaChangesIncludesDerivedConstraints(t *testing.T) {
	users := MustSchema("users", advancedUserSchema{})
	schemaDialect := newFakeSchemaDialect()
	session := &fakeSession{dialect: schemaDialect}

	plan, err := PlanSchemaChanges(context.Background(), session, users)
	if err != nil {
		t.Fatalf("PlanSchemaChanges returned error: %v", err)
	}

	if plan.Actions.Len() != 2 {
		t.Fatalf("unexpected action count: %d", plan.Actions.Len())
	}
	first, _ := plan.Actions.Get(0)
	if first.Kind != schemax.MigrationActionCreateTable {
		t.Fatalf("unexpected first action: %+v", first)
	}
	second, _ := plan.Actions.Get(1)
	if second.Kind != schemax.MigrationActionCreateIndex {
		t.Fatalf("unexpected second action: %+v", second)
	}
	if report := plan.Report; report.Tables.Len() != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	table, _ := plan.Report.Tables.Get(0)
	if table.PrimaryKeyDiff == nil {
		t.Fatalf("unexpected report: %+v", plan.Report)
	}
	preview := plan.SQLPreview()
	if preview.Len() != 2 {
		t.Fatalf("unexpected preview count: %d", preview.Len())
	}
	firstPreview, _ := preview.Get(0)
	if !strings.Contains(firstPreview, "create table users") {
		t.Fatalf("unexpected preview sql: %+v", preview)
	}
}

func TestAutoMigrateAddsMissingForeignKeyAndCheck(t *testing.T) {
	users := MustSchema("users", advancedUserSchema{})
	schemaDialect := newFakeSchemaDialect()
	schemaDialect.tables["users"] = schemax.TableState{
		Exists: true,
		Name:   "users",
		Columns: collectionx.NewList[schemax.ColumnState](
			schemax.ColumnState{Name: "id", Type: "bigint", Nullable: false},
			schemax.ColumnState{Name: "tenant_id", Type: "bigint", Nullable: false},
			schemax.ColumnState{Name: "username", Type: "text", Nullable: false},
			schemax.ColumnState{Name: "status", Type: "integer", Nullable: false},
			schemax.ColumnState{Name: "role_id", Type: "bigint", Nullable: false},
		),
		Indexes:    toIndexStates(IndexesForTest(users)),
		PrimaryKey: &schemax.PrimaryKeyState{Name: "pk_users", Columns: collectionx.NewList[string]("id", "tenant_id")},
	}
	session := &fakeSession{dialect: schemaDialect}

	report, err := AutoMigrate(context.Background(), session, users)
	if err != nil {
		t.Fatalf("AutoMigrate returned error: %v", err)
	}
	if !report.Valid() {
		t.Fatalf("expected valid report: %+v", report)
	}
	if schemaDialect.tables["users"].ForeignKeys.Len() != 1 {
		t.Fatalf("expected derived foreign key to be created: %+v", schemaDialect.tables["users"].ForeignKeys)
	}
	if schemaDialect.tables["users"].Checks.Len() != 1 {
		t.Fatalf("expected check constraint to be created: %+v", schemaDialect.tables["users"].Checks)
	}
}

type failingIndexDialect struct {
}

func (failingIndexDialect) Name() string         { return "failing-sqlite" }
func (failingIndexDialect) BindVar(_ int) string { return "?" }
func (failingIndexDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
func (failingIndexDialect) RenderLimitOffset(limit, offset *int) (string, error) {
	return testSQLiteDialect{}.RenderLimitOffset(limit, offset)
}
func (failingIndexDialect) NormalizeType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
func (d failingIndexDialect) BuildCreateTable(spec schemax.TableSpec) (sqlstmt.Bound, error) {
	parts := make([]string, 0, spec.Columns.Len())
	singlePK := ""
	if spec.PrimaryKey != nil && spec.PrimaryKey.Columns.Len() == 1 {
		singlePK, _ = spec.PrimaryKey.Columns.GetFirst()
	}
	columns := spec.Columns.Values()
	for index := range columns {
		column := columns[index]
		typeName := column.SQLType
		if typeName == "" {
			typeName = InferTypeNameForTest(column)
		}
		part := d.QuoteIdent(column.Name) + " " + strings.ToUpper(typeName)
		if column.Name == singlePK {
			part += " PRIMARY KEY"
		} else if !column.Nullable {
			part += " NOT NULL"
		}
		parts = append(parts, part)
	}
	return sqlstmt.Bound{SQL: "CREATE TABLE IF NOT EXISTS " + d.QuoteIdent(spec.Name) + " (" + strings.Join(parts, ", ") + ")"}, nil
}
func (d failingIndexDialect) BuildAddColumn(table string, column schemax.ColumnMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{}, fmt.Errorf("unexpected add column for test table %s column %s", table, column.Name)
}
func (d failingIndexDialect) BuildCreateIndex(index schemax.IndexMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{SQL: "CREATE INDEX broken syntax"}, nil
}
func (d failingIndexDialect) BuildAddForeignKey(table string, foreignKey schemax.ForeignKeyMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{}, fmt.Errorf("unexpected add foreign key for test table %s constraint %s", table, foreignKey.Name)
}
func (d failingIndexDialect) BuildAddCheck(table string, check schemax.CheckMeta) (sqlstmt.Bound, error) {
	return sqlstmt.Bound{}, fmt.Errorf("unexpected add check for test table %s check %s", table, check.Name)
}
func (d failingIndexDialect) InspectTable(ctx context.Context, executor Executor, table string) (state schemax.TableState, err error) {
	rows, err := executor.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table)
	if err != nil {
		return schemax.TableState{}, fmt.Errorf("inspect test table %s: %w", table, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	exists := rows.Next()
	if iterErr := rows.Err(); iterErr != nil {
		return schemax.TableState{}, fmt.Errorf("inspect test table %s rows: %w", table, iterErr)
	}
	if !exists {
		return schemax.TableState{Name: table, Exists: false}, nil
	}
	return schemax.TableState{Name: table, Exists: true}, nil
}

func TestAutoMigrateRollsBackTransactionalDDLOnFailure(t *testing.T) {
	ctx := context.Background()
	raw, cleanup := OpenTestSQLite(t)
	defer cleanup()

	core := MustNewWithOptions(raw, failingIndexDialect{})
	users := MustSchema("users", UserSchema{})

	_, err := AutoMigrate(ctx, core, users)
	if err == nil {
		t.Fatal("expected automigrate to fail on invalid index SQL")
	}

	var exists bool
	if scanErr := raw.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)`, "users").Scan(&exists); scanErr != nil {
		t.Fatalf("inspect sqlite_master: %v", scanErr)
	}
	if exists {
		t.Fatal("expected users table creation to roll back after transactional automigrate failure")
	}
}

func TestAutoMigrateWarnsWhenTransactionSupportIsUnavailable(t *testing.T) {
	users := MustSchema("users", UserSchema{})
	schemaDialect := newFakeSchemaDialect()
	session := &fakeSession{dialect: schemaDialect}

	report, err := AutoMigrate(context.Background(), session, users)
	if err != nil {
		t.Fatalf("AutoMigrate returned error: %v", err)
	}
	if !report.HasWarnings() {
		t.Fatal("expected auto migrate warning without transaction support")
	}
	if !strings.Contains(strings.Join(report.Warnings.Values(), " "), "without transaction") {
		t.Fatalf("expected non-transactional warning, got: %+v", report.Warnings)
	}
}
