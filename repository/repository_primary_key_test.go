package repository_test

import (
	"testing"

	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

func TestBaseByIDUsesPrimaryKeyColumnFromSchema(t *testing.T) {
	repo, devices, ctx := newDeviceRepo(t, "file:repository_pk_column_test?mode=memory&cache=shared")
	byDeviceID := repository.By(repo, devices.DeviceID)
	require.NoError(t, repo.Create(ctx, &Device{DeviceID: "dev-1", Name: "sensor"}))

	item, err := byDeviceID.Get(ctx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor", item.Name)

	_, err = byDeviceID.Update(ctx, "dev-1", devices.Name.Set("sensor-v2"))
	require.NoError(t, err)

	updated, err := byDeviceID.Get(ctx, "dev-1")
	require.NoError(t, err)
	require.Equal(t, "sensor-v2", updated.Name)

	_, err = byDeviceID.Delete(ctx, "dev-1")
	require.NoError(t, err)

	_, err = byDeviceID.Get(ctx, "dev-1")
	require.ErrorIs(t, err, repository.ErrNotFound)
}
