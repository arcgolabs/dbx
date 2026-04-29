package schemamigrate_test

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect"
	relationx "github.com/arcgolabs/dbx/relation"
	schemax "github.com/arcgolabs/dbx/schema"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
)

type Executor = dbx.Executor
type Row = dbx.Row
type SchemaResource = schemamigrate.Resource

type Schema[E any] = schemax.Schema[E]
type Column[E any, T any] = columnx.Column[E, T]
type Index[E any] = schemax.Index[E]
type CompositeKey[E any] = schemax.CompositeKey[E]
type Check[E any] = schemax.Check[E]

type Role struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type User struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email_address"`
	Status   int    `dbx:"status"`
	RoleID   int64  `dbx:"role_id"`
	Ignored  string `dbx:"ignored"`
}

type RoleSchema struct {
	Schema[Role]
	ID   Column[Role, int64]  `dbx:"id,pk"`
	Name Column[Role, string] `dbx:"name,unique"`
}

type UserSchema struct {
	Schema[User]
	ID       Column[User, int64] `dbx:"id,pk"`
	Username Column[User, string]
	Email    Column[User, string]             `dbx:"email_address,index"`
	Status   Column[User, int]                `dbx:"status"`
	RoleID   Column[User, int64]              `dbx:"role_id,ref=roles.id,ondelete=cascade"`
	Role     relationx.BelongsTo[User, Role]  `rel:"table=roles,local=role_id,target=id"`
	Peer     relationx.HasOne[User, User]     `rel:"table=user_peers,local=id,target=user_id"`
	Children relationx.HasMany[User, User]    `rel:"table=users,local=id,target=parent_id"`
	Roles    relationx.ManyToMany[User, Role] `rel:"table=roles,target=id,join=user_roles,join_local=user_id,join_target=role_id"`
}

var AtlasSplitChangesForTest = schemamigrate.AtlasSplitChangesForTest
var AutoMigrate = schemamigrate.AutoMigrate
var CompileAtlasSchemaForTest = schemamigrate.CompileAtlasSchemaForTest
var MustNewWithOptions = dbx.MustNewWithOptions
var New = dbx.New
var PlanSchemaChanges = schemamigrate.PlanSchemaChanges
var ValidateSchemas = schemamigrate.ValidateSchemas

func ClonePrimaryKeyMetaForTest(meta schemax.PrimaryKeyMeta) schemax.PrimaryKeyMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func ClonePrimaryKeyStateForTest(state schemax.PrimaryKeyState) schemax.PrimaryKeyState {
	state.Columns = state.Columns.Clone()
	return state
}

func IndexesForTest(schema schemax.Resource) *collectionx.List[schemax.IndexMeta] {
	return schema.Spec().Indexes.Clone()
}

func InferTypeNameForTest(column schemax.ColumnMeta) string {
	return schemax.InferTypeName(column)
}

func MustSchema[S any](name string, schema S) S {
	return schemax.MustSchema(name, schema)
}

func OpenTestSQLite(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:schemamigrate-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sql.Open returned error: %v", err)
	}
	return raw, func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Fatalf("raw.Close returned error: %v", closeErr)
		}
	}
}

type testSQLiteDialect struct{}

func (testSQLiteDialect) Name() string         { return "sqlite" }
func (testSQLiteDialect) BindVar(_ int) string { return "?" }
func (testSQLiteDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (testSQLiteDialect) RenderLimitOffset(limit, offset *int) (string, error) {
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

type testPostgresDialect struct{}

func (testPostgresDialect) Name() string { return "postgres" }
func (testPostgresDialect) BindVar(n int) string {
	return "$" + strconv.Itoa(n)
}
func (testPostgresDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
func (testPostgresDialect) RenderLimitOffset(limit, offset *int) (string, error) {
	if limit == nil && offset == nil {
		return "", nil
	}
	if limit != nil && offset != nil {
		return fmt.Sprintf("LIMIT %d OFFSET %d", *limit, *offset), nil
	}
	if limit != nil {
		return fmt.Sprintf("LIMIT %d", *limit), nil
	}
	return fmt.Sprintf("OFFSET %d", *offset), nil
}

type testMySQLDialect struct{}

func (testMySQLDialect) Name() string         { return "mysql" }
func (testMySQLDialect) BindVar(_ int) string { return "?" }
func (testMySQLDialect) QuoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
func (testMySQLDialect) RenderLimitOffset(limit, offset *int) (string, error) {
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

var _ dialect.Dialect = testSQLiteDialect{}
var _ dialect.Dialect = testPostgresDialect{}
var _ dialect.Dialect = testMySQLDialect{}
