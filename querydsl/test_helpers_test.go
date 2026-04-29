package querydsl_test

import (
	"fmt"
	"strconv"
	"strings"

	columnx "github.com/arcgolabs/dbx/column"
	basedialect "github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/querydsl"
	relationx "github.com/arcgolabs/dbx/relation"
	schemax "github.com/arcgolabs/dbx/schema"
)

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
	schemax.Schema[Role]
	ID   columnx.Column[Role, int64]  `dbx:"id,pk"`
	Name columnx.Column[Role, string] `dbx:"name,unique"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.Column[User, int64] `dbx:"id,pk"`
	Username columnx.Column[User, string]
	Email    columnx.Column[User, string]     `dbx:"email_address,index"`
	Status   columnx.Column[User, int]        `dbx:"status"`
	RoleID   columnx.Column[User, int64]      `dbx:"role_id,ref=roles.id,ondelete=cascade"`
	Role     relationx.BelongsTo[User, Role]  `rel:"table=roles,local=role_id,target=id"`
	Peer     relationx.HasOne[User, User]     `rel:"table=user_peers,local=id,target=user_id"`
	Children relationx.HasMany[User, User]    `rel:"table=users,local=id,target=parent_id"`
	Roles    relationx.ManyToMany[User, Role] `rel:"table=roles,target=id,join=user_roles,join_local=user_id,join_target=role_id"`
}

var And = querydsl.And
var AllColumns = querydsl.AllColumns
var CountAll = querydsl.CountAll
var DeleteFrom = querydsl.DeleteFrom
var Exists = querydsl.Exists
var From = querydsl.From
var InsertInto = querydsl.InsertInto
var NamedTable = querydsl.NamedTable
var Select = querydsl.Select
var SelectFrom = querydsl.SelectFrom
var Update = querydsl.Update
var View = querydsl.View

func Alias[S querydsl.TableSource](schema S, alias string) S {
	return querydsl.As(schema, alias)
}

func CaseWhen[T any](predicate querydsl.Predicate, value any) *querydsl.CaseBuilder[T] {
	return querydsl.CaseWhen[T](predicate, value)
}

func Count[E any, T any](expr columnx.Column[E, T]) querydsl.Aggregate[int64] {
	return querydsl.Count(expr)
}

func JoinRelation(query *querydsl.SelectQuery, source relationx.JoinSource, relation relationx.Accessor, target querydsl.TableSource) (*querydsl.SelectQuery, error) {
	joined, err := relationx.Join(query, source, relation, target)
	if err != nil {
		return nil, fmt.Errorf("join relation: %w", err)
	}
	return joined, nil
}

func Like[E any](column columnx.Column[E, string], pattern string) querydsl.Predicate {
	return querydsl.Like(column, pattern)
}

func MustSchema[S any](name string, schema S) S {
	return schemax.MustSchema(name, schema)
}

func NamedColumn[T any](source querydsl.TableSource, name string) columnx.Column[struct{}, T] {
	return columnx.Named[T](source, name)
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

var _ basedialect.Dialect = testSQLiteDialect{}
var _ basedialect.Dialect = testPostgresDialect{}
var _ basedialect.Dialect = testMySQLDialect{}
