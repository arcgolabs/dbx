package dbx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/arcgolabs/dbx/querydsl"
	"strings"
	"testing"
)

type accountLabel string

func (l *accountLabel) Scan(src any) error {
	switch value := src.(type) {
	case string:
		*l = accountLabel(strings.ToUpper(value))
		return nil
	case []byte:
		*l = accountLabel(strings.ToUpper(string(value)))
		return nil
	default:
		return fmt.Errorf("unsupported scan type %T", src)
	}
}

func (l accountLabel) Value() (driver.Value, error) {
	return strings.ToLower(string(l)), nil
}

type AccountProfile struct {
	Nickname *string        `dbx:"nickname"`
	Bio      sql.NullString `dbx:"bio"`
}

type accountRecord struct {
	ID int64 `dbx:"id"`
	*AccountProfile
	Label accountLabel `dbx:"label"`
}

type auditFields struct {
	CreatedBy string `dbx:"created_by"`
	UpdatedBy string `dbx:"updated_by"`
}

type auditedUser struct {
	ID    int64       `dbx:"id"`
	Audit auditFields `dbx:",inline"`
}

type accountSchema struct {
	Schema[accountRecord]
	ID       Column[accountRecord, int64]          `dbx:"id,pk,auto"`
	Nickname Column[accountRecord, *string]        `dbx:"nickname,nullable"`
	Bio      Column[accountRecord, sql.NullString] `dbx:"bio,nullable"`
	Label    Column[accountRecord, accountLabel]   `dbx:"label"`
}

const mapperScanAccountsDDL = `
CREATE TABLE IF NOT EXISTS "accounts" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"nickname" TEXT,
	"bio" TEXT,
	"label" TEXT NOT NULL
);
`

func TestStructMapperScansEmbeddedPointerNullableAndScanner(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, mapperScanAccountsDDL,
		`INSERT INTO "accounts" ("id","nickname","bio","label") VALUES (1,'ally','hello','admin'),(2,NULL,NULL,'reader')`,
	)
	defer cleanup()

	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustStructMapper[accountRecord]()

	items, err := QueryAll[accountRecord](context.Background(), New(sqlDB, testSQLiteDialect{}), Select(AllColumns(accounts).Values()...).From(accounts), mapper)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	assertAccountRecords(t, items.Values())
}

func TestMapperInsertAssignmentsWithNilEmbeddedPointerAndValuer(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, mapperScanAccountsDDL)
	defer cleanup()

	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustMapper[accountRecord](accounts)
	entity := &accountRecord{
		Label: "ADMIN",
	}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), accounts, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if assignments.Len() != 3 {
		t.Fatalf("unexpected assignment count: %d", assignments.Len())
	}

	rec := &hookRecorder{}
	if _, err := Exec(context.Background(), MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after})), InsertInto(accounts).Values(assignments.Values()...)); err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected exec count: %d", rec.execCount)
	}
}

func TestStructMapperSupportsNamedInlineFields(t *testing.T) {
	mapper := MustStructMapper[auditedUser]()

	fields := mapper.Fields()
	if fields.Len() != 3 {
		t.Fatalf("unexpected mapped field count: %d", fields.Len())
	}
	createdBy, ok := mapper.FieldByColumn("created_by")
	if !ok {
		t.Fatal("expected created_by mapping")
	}
	if createdBy.Path.Len() != 2 {
		t.Fatalf("expected inline field path depth=2, got: %+v", createdBy.Path)
	}
	rootIndex, _ := createdBy.Path.Get(0)
	if rootIndex != 1 {
		t.Fatalf("unexpected inline field root path: %+v", createdBy.Path)
	}
}

func TestStructMapperScanPlanMatchesQualifiedAndCaseInsensitiveColumns(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('a','a@x.com',1,1),('b','b@x.com',1,1)`,
	)
	defer cleanup()

	type aggregateRow struct {
		ID        int64 `dbx:"id"`
		UserCount int64 `dbx:"user_count"`
	}

	users := MustSchema("users", UserSchema{})
	items, err := QueryAll[aggregateRow](
		context.Background(),
		New(sqlDB, testSQLiteDialect{}),
		Select(users.ID, CountAll().As("user_count")).From(users),
		MustStructMapper[aggregateRow](),
	)
	if err != nil {
		t.Fatalf("QueryAll returned error: %v", err)
	}
	if items.Len() != 1 {
		t.Fatalf("unexpected item count: %d", items.Len())
	}
	item, _ := items.GetFirst()
	if item.ID != 1 || item.UserCount != 2 {
		t.Fatalf("unexpected aggregate row: %+v", item)
	}
}

func TestQueryCursorScansEmbeddedPointerNullableAndScanner(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t, mapperScanAccountsDDL,
		`INSERT INTO "accounts" ("id","nickname","bio","label") VALUES (1,'ally','hello','admin'),(2,NULL,NULL,'reader')`,
	)
	defer cleanup()

	accounts := MustSchema("accounts", accountSchema{})
	mapper := MustStructMapper[accountRecord]()
	core := New(sqlDB, testSQLiteDialect{})
	query := Select(AllColumns(accounts).Values()...).From(accounts)

	cursor, err := QueryCursor[accountRecord](context.Background(), core, query, mapper)
	if err != nil {
		t.Fatalf("QueryCursor returned error: %v", err)
	}
	defer closeCursorOrFatal(t, cursor)

	assertAccountRecords(t, collectAccountCursor(t, cursor))
	assertAccountRecords(t, collectAccountEach(t, core, query, mapper))
}

func assertAccountRecords(t *testing.T, items []accountRecord) {
	t.Helper()
	if len(items) != 2 {
		t.Fatalf("unexpected item count: %d", len(items))
	}
	assertFirstAccountRecord(t, items[0])
	assertSecondAccountRecord(t, items[1])
}

func collectAccountCursor(t *testing.T, cursor Cursor[accountRecord]) []accountRecord {
	t.Helper()
	var items []accountRecord
	for cursor.Next() {
		item, err := cursor.Get()
		if err != nil {
			t.Fatalf("cursor.Get returned error: %v", err)
		}
		items = append(items, item)
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor.Err returned error: %v", err)
	}
	return items
}

func collectAccountEach(t *testing.T, core *DB, query *querydsl.SelectQuery, mapper StructMapper[accountRecord]) []accountRecord {
	t.Helper()
	var items []accountRecord
	QueryEach[accountRecord](context.Background(), core, query, mapper)(func(item accountRecord, err error) bool {
		if err != nil {
			t.Fatalf("QueryEach yielded error: %v", err)
		}
		items = append(items, item)
		return true
	})
	return items
}

func assertFirstAccountRecord(t *testing.T, item accountRecord) {
	t.Helper()
	if item.AccountProfile == nil || item.Nickname == nil || *item.Nickname != "ally" {
		t.Fatalf("unexpected first item: %+v", item)
	}
	if !item.Bio.Valid || item.Bio.String != "hello" {
		t.Fatalf("unexpected bio: %+v", item.Bio)
	}
	if item.Label != "ADMIN" {
		t.Fatalf("unexpected custom scanner label: %q", item.Label)
	}
}

func assertSecondAccountRecord(t *testing.T, item accountRecord) {
	t.Helper()
	if item.Nickname != nil {
		t.Fatalf("expected nil nickname, got: %+v", item.Nickname)
	}
	if item.Bio.Valid {
		t.Fatalf("expected invalid bio, got: %+v", item.Bio)
	}
	if item.Label != "READER" {
		t.Fatalf("unexpected second label: %q", item.Label)
	}
}
