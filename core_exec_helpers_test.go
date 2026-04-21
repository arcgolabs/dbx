package dbx_test

import (
	"context"
	"github.com/arcgolabs/dbx/querydsl"
	"testing"

	"github.com/DaiYuANg/arcgo/collectionx"
)

func closeCursorOrFatal[E any](t *testing.T, cursor Cursor[E]) {
	t.Helper()
	if closeErr := cursor.Close(); closeErr != nil {
		t.Fatalf("cursor.Close returned error: %v", closeErr)
	}
}

func collectUserSummaryCursor(t *testing.T, cursor Cursor[UserSummary]) []UserSummary {
	t.Helper()
	var items []UserSummary
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

func collectUserSummaryEach(t *testing.T, core *DB, query *querydsl.SelectQuery, mapper StructMapper[UserSummary]) []UserSummary {
	t.Helper()
	var items []UserSummary
	QueryEach[UserSummary](context.Background(), core, query, mapper)(func(item UserSummary, err error) bool {
		if err != nil {
			t.Fatalf("QueryEach yielded error: %v", err)
		}
		items = append(items, item)
		return true
	})
	return items
}

func assertUserSummaryRows(t *testing.T, items []UserSummary) {
	t.Helper()
	if len(items) != 2 || items[0].Username != "alice" || items[1].ID != 2 {
		t.Fatalf("unexpected items: %+v", items)
	}
}

func mustInsertAssignments(t *testing.T, mapper Mapper[User], users UserSchema, entity *User) collectionx.List[querydsl.Assignment] {
	t.Helper()
	assignments, err := mapper.InsertAssignments(New(nil, testSQLiteDialect{}), users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	return assignments
}

func mustUpdateAssignments(t *testing.T, mapper Mapper[User], users UserSchema, entity *User) collectionx.List[querydsl.Assignment] {
	t.Helper()
	assignments, err := mapper.UpdateAssignments(users, entity)
	if err != nil {
		t.Fatalf("UpdateAssignments returned error: %v", err)
	}
	return assignments
}

func mustPrimaryPredicate(t *testing.T, mapper Mapper[User], users UserSchema, entity *User) querydsl.Predicate {
	t.Helper()
	predicate, err := mapper.PrimaryPredicate(users, entity)
	if err != nil {
		t.Fatalf("PrimaryPredicate returned error: %v", err)
	}
	return predicate
}
