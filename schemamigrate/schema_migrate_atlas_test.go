package schemamigrate_test

import (
	"context"
	"database/sql"
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"
	"testing"

	atlasschema "ariga.io/atlas/sql/schema"
	_ "modernc.org/sqlite"
)

func TestCompileAtlasSchemaIncludesDerivedMetadata(t *testing.T) {
	users := MustSchema("users", advancedUserSchema{})
	compiled := CompileAtlasSchemaForTest("sqlite", users)
	if compiled == nil || compiled.Schema == nil {
		t.Fatal("expected compiled atlas schema")
	}
	if len(compiled.Schema.Tables) != 1 {
		t.Fatalf("unexpected table count: %d", len(compiled.Schema.Tables))
	}
	table := compiled.Schema.Tables[0]
	if table.PrimaryKey == nil || len(table.PrimaryKey.Parts) != 2 {
		t.Fatalf("expected composite primary key, got: %+v", table.PrimaryKey)
	}
	if len(table.Indexes) != 1 {
		t.Fatalf("expected one secondary index, got: %d", len(table.Indexes))
	}
	if len(table.ForeignKeys) != 1 {
		t.Fatalf("expected one derived foreign key, got: %d", len(table.ForeignKeys))
	}
	if len(table.Attrs) != 1 {
		t.Fatalf("expected one check constraint attr, got: %d", len(table.Attrs))
	}
}

func TestAtlasSplitChangesSeparatesExecutableAndManualChanges(t *testing.T) {
	users := MustSchema("users", advancedUserSchema{})
	compiled := CompileAtlasSchemaForTest("sqlite", users)
	compiledTable, ok := compiled.Table("users")
	if !ok {
		t.Fatal("expected compiled users table")
	}
	changes := []atlasschema.Change{
		&atlasschema.ModifyTable{T: compiledTable, Changes: []atlasschema.Change{
			&atlasschema.AddColumn{C: atlasschema.NewStringColumn("nickname", "text")},
			&atlasschema.AddPrimaryKey{P: atlasschema.NewPrimaryKey(atlasschema.NewColumn("id"))},
		}},
	}
	safe, manual := AtlasSplitChangesForTest(changes)
	if len(safe) != 1 {
		t.Fatalf("expected one executable atlas change, got: %d", len(safe))
	}
	if len(manual) != 1 {
		t.Fatalf("expected one manual action, got: %d", len(manual))
	}
	if manual[0].Kind != schemax.MigrationActionManual {
		t.Fatalf("unexpected manual action kind: %+v", manual[0])
	}
}

func TestPlanSchemaChangesWithAtlasIncludesSQLPreview(t *testing.T) {
	raw, err := sql.Open("sqlite", "file:dbx-atlas-preview?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sql.Open returned error: %v", err)
	}
	defer func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Fatalf("raw.Close returned error: %v", closeErr)
		}
	}()

	core := New(raw, testSQLiteDialect{})
	users := MustSchema("users", UserSchema{})

	plan, err := PlanSchemaChanges(context.Background(), core, users)
	if err != nil {
		t.Fatalf("PlanSchemaChanges returned error: %v", err)
	}

	preview := plan.SQLPreview()
	first, ok := preview.Get(0)
	if !ok {
		t.Fatal("expected atlas preview sql")
	}
	if !strings.Contains(strings.ToLower(first), "create table") {
		t.Fatalf("unexpected atlas preview sql: %+v", preview)
	}
	if !strings.Contains(strings.ToLower(strings.Join(preview.Values(), " ")), "integer not null primary key autoincrement") {
		t.Fatalf("expected sqlite autoincrement preview to use integer primary key: %+v", preview)
	}
}
