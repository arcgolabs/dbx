package repository_test

import (
	"testing"

	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

func TestBaseByIDUsesPrimaryKeyColumnFromSchema(t *testing.T) {
	repo, devices, ctx := newDeviceRepo(t, "file:repository_pk_column_test?mode=memory&cache=shared")
	require.NoError(t, repo.Create(ctx, &Device{DeviceID: "dev-1", Name: "sensor"}))

	item, err := repo.GetByID(ctx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor", item.Name)

	_, err = repo.UpdateByID(ctx, "dev-1", devices.Name.Set("sensor-v2"))
	require.NoError(t, err)

	updated, err := repo.GetByID(ctx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor-v2", updated.Name)

	_, err = repo.DeleteByID(ctx, "dev-1")
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, "dev-1")
	require.ErrorIs(t, err, repository.ErrNotFound)
}
