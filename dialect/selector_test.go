package dialect_test

import (
	"testing"

	"github.com/arcgolabs/dbx/dialect"
	"github.com/stretchr/testify/require"
)

func TestSelectorAliasesAndQueryFeatures(t *testing.T) {
	selector, err := dialect.ParseSelector("PostgreSQL")
	require.NoError(t, err)
	require.Equal(t, dialect.SelectorPostgres, selector)

	features := dialect.DefaultQueryFeatures("postgresql")
	require.Equal(t, "on_conflict", features.UpsertVariant)
	require.True(t, features.SupportsReturning)

	_, err = dialect.ParseSelector("oracle")
	require.Error(t, err)
}
