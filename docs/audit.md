# Audit

`github.com/arcgolabs/dbx/audit` is an independent Go module for Envers-style
audit rows without database triggers.

The audit module is intentionally explicit:

- revision tables are declared with `schema`
- audit tables are declared with `schema`
- entity-to-audit column copies are declared once through `audit.Entity`
- repository automation is opt-in through `repository.WithAuditWriter`
- low-level mutation code can call `audit.Writer` directly

## Schema Binding

```go
revisions := schemax.MustSchema("audit_revisions", RevisionSchema{})
userAudits := schemax.MustSchema("user_audits", UserAuditSchema{})

revision := audit.RevisionTable(
	revisions,
	revisions.ID,
	revisions.CreatedAt,
	audit.RevisionActor(revisions.Actor),
	audit.RevisionReason(revisions.Reason),
)

userAudit := audit.MustEntity[User](
	revision,
	users,
	userAudits,
	audit.AuditRevisionID(userAudits.RevisionID),
	audit.OperationColumn(userAudits.Operation),
	audit.Key(users.ID, userAudits.UserID),
	audit.Copy(users.Name, userAudits.Name),
)
```

## Repository Automation

```go
repo := repository.NewWithOptions[User](
	db,
	users,
	repository.WithAuditWriter(userAudit),
)
```

Repository audit is written for helpers that have a concrete entity snapshot:
`Create`, `CreateMany`, `Upsert`, key-based update/delete, optimistic-lock
update, soft delete, and entity-typed `RETURNING` helpers.

Generic `Update(query)` and `Delete(query)` stay low-level and are not audited
automatically because they do not expose per-row entity snapshots.

## Revision Scope

Without a revision scope, each audit write creates its own revision row. Use
`audit.WithRevisionScope(ctx)` when several writes should share one revision:

```go
ctx := audit.WithRevisionScope(ctx)
err := repo.InTx(ctx, nil, func(tx *dbx.Tx, txRepo *repository.Base[User, UserSchema]) error {
	if err := txRepo.Create(ctx, &user); err != nil {
		return err
	}
	_, err := txRepo.UpdateByKey(ctx, repository.Key{"id": user.ID}, users.Name.Set("alice-v2"))
	return err
})
```

For non-repository code, `audit.InTx` starts a transaction and adds a revision
scope to the callback context:

```go
err := audit.InTx(ctx, db, nil, func(ctx context.Context, tx *dbx.Tx) error {
	return userAudit.Writer().Update(ctx, tx, &user)
})
```

## Metadata

Revision metadata is carried by context and written only when the revision
schema maps the corresponding column:

```go
ctx = audit.WithActor(ctx, actorID)
ctx = audit.WithReason(ctx, "profile update")
ctx = audit.WithTenant(ctx, tenantID)
ctx = audit.WithMetadata(ctx, json.RawMessage(payload))
```
