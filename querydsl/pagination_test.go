package querydsl_test

import (
	"testing"

	"github.com/arcgolabs/dbx/paging"
	"github.com/stretchr/testify/require"
)

func TestSelectQueryPageAppliesPagingRequest(t *testing.T) {
	request := paging.Page(0, 0).WithMaxPageSize(10)
	require.Equal(t, 1, request.Page)
	require.Equal(t, 10, request.PageSize)
	require.Equal(t, 10, request.Limit())
	require.Equal(t, 0, request.Offset())

	users := MustSchema("users", UserSchema{})
	query := Select(users.ID, users.Username).
		From(users).
		OrderBy(users.ID.Asc()).
		PageBy(2, 5)

	bound, err := query.Build(testSQLiteDialect{})
	require.NoError(t, err)
	require.Equal(t, `SELECT "users"."id", "users"."username" FROM "users" ORDER BY "users"."id" ASC LIMIT 5 OFFSET 5`, bound.SQL)
}
