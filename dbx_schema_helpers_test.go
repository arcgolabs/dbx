package dbx_test

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"testing"
)

func assertUserSchemaBasics(t *testing.T, users UserSchema) {
	t.Helper()
	if users.TableName() != "users" {
		t.Fatalf("unexpected table name: %q", users.TableName())
	}
	if users.ID.Ref() != "users.id" {
		t.Fatalf("unexpected id ref: %q", users.ID.Ref())
	}
	if users.Email.Name() != "email_address" {
		t.Fatalf("unexpected email column name: %q", users.Email.Name())
	}
	if !users.ID.IsPrimaryKey() || !users.ID.Meta().AutoIncrement {
		t.Fatalf("expected id metadata to mark pk/auto: %+v", users.ID.Meta())
	}
	if users.ID.Meta().IDStrategy != IDStrategyDBAuto {
		t.Fatalf("expected default int64 id strategy db_auto, got: %q", users.ID.Meta().IDStrategy)
	}
}

func assertUserReferenceMetadata(t *testing.T, users UserSchema) {
	t.Helper()
	ref, ok := users.RoleID.Reference()
	if !ok {
		t.Fatal("expected role_id reference metadata")
	}
	if ref.TargetTable != "roles" || ref.TargetColumn != "id" || ref.OnDelete != schemax.ReferentialCascade {
		t.Fatalf("unexpected reference metadata: %+v", ref)
	}
}

func assertUserRelationMetadata(t *testing.T, users UserSchema) {
	t.Helper()
	columns := users.Columns()
	if columns.Len() != 5 {
		t.Fatalf("unexpected columns metadata count: %d", columns.Len())
	}
	relations := users.Relations()
	if relations.Len() != 4 {
		t.Fatalf("unexpected relations metadata count: %d", relations.Len())
	}
	first, ok := relations.Get(0)
	if !ok || first.Kind != schemax.RelationBelongsTo || first.TargetTable != "roles" {
		t.Fatalf("unexpected first relation metadata: %+v", first)
	}
	last, ok := relations.Get(3)
	if !ok || last.Kind != schemax.RelationManyToMany || last.ThroughTable != "user_roles" {
		t.Fatalf("unexpected many-to-many metadata: %+v", last)
	}
}

func assertUserForeignKeys(t *testing.T, users UserSchema) {
	t.Helper()
	foreignKeys := users.ForeignKeys()
	if foreignKeys.Len() != 1 {
		t.Fatalf("unexpected foreign key count: %d", foreignKeys.Len())
	}
	foreignKey, ok := foreignKeys.Get(0)
	if !ok || foreignKey.Columns.Len() != 1 || foreignKey.Columns.Values()[0] != "role_id" || foreignKey.TargetTable != "roles" {
		t.Fatalf("unexpected foreign key metadata: %+v", foreignKey)
	}
}
