package dbx_test

import (
	"context"
	"testing"
)

func TestExecBuildsAndRunsBoundQuery(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t, `INSERT INTO "roles" ("id","name") VALUES (9,'admin')`)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	mapper := MustMapper[User](users)
	entity := &User{
		Username: "alice",
		Email:    "alice@example.com",
		Status:   1,
		RoleID:   9,
	}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}

	rec := &hookRecorder{}
	result, err := Exec(context.Background(), MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after})), InsertInto(users).Values(assignments.Values()...))
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected returned error: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("unexpected rows affected: %d", rowsAffected)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected recorded exec count: %d", rec.execCount)
	}
}

func TestBeginTxExecsWithinTransaction(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r1')`,
		`INSERT INTO "users" ("username","email_address","status","role_id") VALUES ('u','e@x.com',1,1)`,
	)
	defer cleanup()

	users := MustSchema("users", UserSchema{})
	rec := &hookRecorder{}
	core := MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{AfterFunc: rec.after}))
	tx, err := core.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx returned error: %v", err)
	}

	result, err := Exec(context.Background(), tx, Update(users).Set(users.Status.Set(2)).Where(users.ID.Eq(1)))
	if err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
	commitErr := tx.Commit()
	if commitErr != nil {
		t.Fatalf("Commit returned error: %v", commitErr)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected returned error: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("unexpected rows affected: %d", rowsAffected)
	}
	if rec.execCount != 1 {
		t.Fatalf("unexpected recorded exec count: %d", rec.execCount)
	}
}

func TestInsertAssignmentsGenerateSnowflakeID(t *testing.T) {
	users := MustSchema("users", SnowflakeUserSchema{})
	mapper := MustMapper[SnowflakeUser](users)
	entity := &SnowflakeUser{Username: "alice"}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if entity.ID == 0 {
		t.Fatal("expected generated snowflake id")
	}
	if assignments.Len() != 2 {
		t.Fatalf("expected id + username assignments, got %d", assignments.Len())
	}
}

func TestInsertAssignmentsGenerateUUIDv7ID(t *testing.T) {
	users := MustSchema("users", UUIDUserSchema{})
	mapper := MustMapper[UUIDUser](users)
	entity := &UUIDUser{Username: "alice"}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if entity.ID == "" {
		t.Fatal("expected generated uuid id")
	}
	if assignments.Len() != 2 {
		t.Fatalf("expected id + username assignments, got %d", assignments.Len())
	}
}

func TestInsertAssignmentsGenerateULID(t *testing.T) {
	users := MustSchema("users", ULIDUserSchema{})
	mapper := MustMapper[ULIDUser](users)
	entity := &ULIDUser{Username: "alice"}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if entity.ID == "" {
		t.Fatal("expected generated ulid")
	}
	if assignments.Len() != 2 {
		t.Fatalf("expected id + username assignments, got %d", assignments.Len())
	}
}

func TestInsertAssignmentsGenerateKSUID(t *testing.T) {
	users := MustSchema("users", KSUIDUserSchema{})
	mapper := MustMapper[KSUIDUser](users)
	entity := &KSUIDUser{Username: "alice"}

	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	if entity.ID == "" {
		t.Fatal("expected generated ksuid")
	}
	if assignments.Len() != 2 {
		t.Fatalf("expected id + username assignments, got %d", assignments.Len())
	}
}
