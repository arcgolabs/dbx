package shared

import (
	"context"
	"fmt"

	"github.com/DaiYuANg/arcgo/dbx"
	mapperx "github.com/DaiYuANg/arcgo/dbx/mapper"
	"github.com/DaiYuANg/arcgo/dbx/querydsl"
	schemax "github.com/DaiYuANg/arcgo/dbx/schema"
)

// InsertAll inserts multiple items for a schema using dbx mapper assignments.
func InsertAll[E any, S schemax.SchemaSource[E]](ctx context.Context, session dbx.Session, schema S, items ...E) error {
	mapper := mapperx.MustMapper[E](schema)
	for _, item := range items {
		assignments, err := mapper.InsertAssignments(session, schema, new(item))
		if err != nil {
			return fmt.Errorf("build insert assignments: %w", err)
		}
		if _, err := dbx.Exec(ctx, session, querydsl.InsertInto(schema).Values(assignments.Values()...)); err != nil {
			return fmt.Errorf("execute insert: %w", err)
		}
	}
	return nil
}

// SeedDemoData loads the shared demo dataset used by dbx examples.
func SeedDemoData(ctx context.Context, session dbx.Session, catalog Catalog) error {
	if err := InsertAll(ctx, session, catalog.Roles,
		Role{Name: "admin"},
		Role{Name: "reader"},
		Role{Name: "auditor"},
	); err != nil {
		return err
	}

	if err := InsertAll(ctx, session, catalog.Users,
		User{Username: "alice", Email: "alice@example.com", Status: 1, RoleID: 1},
		User{Username: "bob", Email: "bob@example.com", Status: 1, RoleID: 2},
		User{Username: "carol", Email: "carol@example.com", Status: 0, RoleID: 3},
	); err != nil {
		return err
	}

	return InsertAll(ctx, session, catalog.UserRoles,
		UserRoleLink{UserID: 1, RoleID: 1},
		UserRoleLink{UserID: 1, RoleID: 2},
		UserRoleLink{UserID: 2, RoleID: 2},
		UserRoleLink{UserID: 3, RoleID: 3},
	)
}
