package repository_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/arcgolabs/dbx"
	repository "github.com/arcgolabs/dbx/repository"
	"github.com/stretchr/testify/require"
)

type auditEvent struct {
	Operation string
	DeviceID  string
	Name      string
}

type deviceAuditWriter struct {
	events []auditEvent
}

func (w *deviceAuditWriter) WriteAudit(_ context.Context, _ dbx.Session, operation string, entity any) error {
	device, ok := entity.(*Device)
	if !ok {
		return fmt.Errorf("unexpected audit entity %T", entity)
	}
	w.events = append(w.events, auditEvent{
		Operation: operation,
		DeviceID:  device.DeviceID,
		Name:      device.Name,
	})
	return nil
}

func TestRepositoryAuditWriterReceivesEntitySnapshots(t *testing.T) {
	baseRepo, devices, ctx := newDeviceRepo(t, "file:repository_audit_writer_test?mode=memory&cache=shared")
	writer := &deviceAuditWriter{}
	repo := repository.NewWithOptions[Device](baseRepo.DB(), devices, repository.WithAuditWriter(writer))

	require.NoError(t, repo.Create(ctx, &Device{DeviceID: "phone", Name: "Phone"}))
	_, err := repo.UpdateByKey(ctx, repository.Key{"device_id": "phone"}, devices.Name.Set("Phone v2"))
	require.NoError(t, err)
	_, err = repo.DeleteByKey(ctx, repository.Key{"device_id": "phone"})
	require.NoError(t, err)

	require.Equal(t, []auditEvent{
		{Operation: "insert", DeviceID: "phone", Name: "Phone"},
		{Operation: "update", DeviceID: "phone", Name: "Phone v2"},
		{Operation: "delete", DeviceID: "phone", Name: "Phone v2"},
	}, writer.events)
}
