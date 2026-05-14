package audit_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/arcgolabs/dbx"
	auditx "github.com/arcgolabs/dbx/audit"
	columnx "github.com/arcgolabs/dbx/column"
	sqlitedialect "github.com/arcgolabs/dbx/dialect/sqlite"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

const auditDDL = `
CREATE TABLE users (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL
);
CREATE TABLE audit_revisions (
	id INTEGER PRIMARY KEY,
	created_at TIMESTAMP NOT NULL,
	actor TEXT,
	reason TEXT
);
CREATE TABLE user_audits (
	revision_id INTEGER NOT NULL,
	operation TEXT NOT NULL,
	user_id INTEGER NOT NULL,
	name TEXT NOT NULL
);
`

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.Column[User, int64]  `dbx:"id,pk"`
	Name columnx.Column[User, string] `dbx:"name"`
}

type RevisionRow struct {
	ID        int64     `dbx:"id"`
	CreatedAt time.Time `dbx:"created_at"`
	Actor     string    `dbx:"actor"`
	Reason    string    `dbx:"reason"`
}

type RevisionSchema struct {
	schemax.Schema[RevisionRow]
	ID        columnx.Column[RevisionRow, int64]     `dbx:"id,pk"`
	CreatedAt columnx.Column[RevisionRow, time.Time] `dbx:"created_at"`
	Actor     columnx.Column[RevisionRow, string]    `dbx:"actor"`
	Reason    columnx.Column[RevisionRow, string]    `dbx:"reason"`
}

type UserAuditRow struct {
	RevisionID int64  `dbx:"revision_id"`
	Operation  string `dbx:"operation"`
	UserID     int64  `dbx:"user_id"`
	Name       string `dbx:"name"`
}

type UserAuditSchema struct {
	schemax.Schema[UserAuditRow]
	RevisionID columnx.Column[UserAuditRow, int64]  `dbx:"revision_id"`
	Operation  columnx.Column[UserAuditRow, string] `dbx:"operation"`
	UserID     columnx.Column[UserAuditRow, int64]  `dbx:"user_id"`
	Name       columnx.Column[UserAuditRow, string] `dbx:"name"`
}

func TestWriterWritesEntitySnapshots(t *testing.T) {
	raw, cleanup := openAuditSQLite(t)
	defer cleanup()
	core := dbx.New(raw, sqlitedialect.New())
	spec := auditFixture(t)
	writer := spec.Writer()
	ctx := auditx.WithReason(auditx.WithActor(context.Background(), "tester"), "case-1")

	require.NoError(t, writer.Insert(ctx, core, &User{ID: 1, Name: "alice"}))
	require.NoError(t, writer.Update(ctx, core, &User{ID: 1, Name: "alice2"}))
	require.NoError(t, writer.Delete(ctx, core, &User{ID: 1, Name: "alice2"}))

	rows, err := raw.QueryContext(ctx, `SELECT revision_id, operation, user_id, name FROM user_audits ORDER BY revision_id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var got []string
	for rows.Next() {
		var revisionID, userID int64
		var operation, name string
		require.NoError(t, rows.Scan(&revisionID, &operation, &userID, &name))
		got = append(got, operation+":"+name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"insert:alice", "update:alice2", "delete:alice2"}, got)

	var actor, reason string
	require.NoError(t, raw.QueryRowContext(ctx, `SELECT actor, reason FROM audit_revisions WHERE id = 1`).Scan(&actor, &reason))
	require.Equal(t, "tester", actor)
	require.Equal(t, "case-1", reason)
}

func TestWriterReusesRevisionScope(t *testing.T) {
	raw, cleanup := openAuditSQLite(t)
	defer cleanup()
	core := dbx.New(raw, sqlitedialect.New())
	spec := auditFixture(t)
	writer := spec.Writer()
	ctx := auditx.WithRevisionScope(context.Background())

	require.NoError(t, writer.Insert(ctx, core, &User{ID: 10, Name: "scoped"}))
	require.NoError(t, writer.Update(ctx, core, &User{ID: 10, Name: "scoped2"}))

	var revisionRows int
	require.NoError(t, raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_revisions`).Scan(&revisionRows))
	require.Equal(t, 1, revisionRows)

	var distinctRevisions int
	require.NoError(t, raw.QueryRowContext(ctx, `SELECT COUNT(DISTINCT revision_id) FROM user_audits`).Scan(&distinctRevisions))
	require.Equal(t, 1, distinctRevisions)
}

func TestLowLevelWriterWorksWithAuditTransaction(t *testing.T) {
	raw, cleanup := openAuditSQLite(t)
	defer cleanup()
	core := dbx.New(raw, sqlitedialect.New())
	spec := auditFixture(t)
	ctx := context.Background()

	err := auditx.InTx(ctx, core, nil, func(ctx context.Context, tx *dbx.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO users(id, name) VALUES(20, 'manual')`); err != nil {
			return fmt.Errorf("insert manual user: %w", err)
		}
		if err := spec.Writer().Insert(ctx, tx, &User{ID: 20, Name: "manual"}); err != nil {
			return fmt.Errorf("write manual audit: %w", err)
		}
		return nil
	})
	require.NoError(t, err)

	var operation, name string
	require.NoError(t, raw.QueryRowContext(ctx, `SELECT operation, name FROM user_audits`).Scan(&operation, &name))
	require.Equal(t, "insert", operation)
	require.Equal(t, "manual", name)
}

func auditFixture(t *testing.T) auditx.EntitySpec[User] {
	t.Helper()
	users := schemax.MustSchema("users", UserSchema{})
	revisions := schemax.MustSchema("audit_revisions", RevisionSchema{})
	userAudits := schemax.MustSchema("user_audits", UserAuditSchema{})
	var nextRevisionID int64
	revision := auditx.RevisionTable(
		revisions,
		revisions.ID,
		revisions.CreatedAt,
		auditx.RevisionActor(revisions.Actor),
		auditx.RevisionReason(revisions.Reason),
		auditx.WithRevisionIDFactory(func(context.Context, dbx.Session) (any, error) {
			nextRevisionID++
			return nextRevisionID, nil
		}),
		auditx.WithClock(func() time.Time { return time.Unix(1000, 0).UTC() }),
	)
	spec, err := auditx.Entity[User](
		revision,
		users,
		userAudits,
		auditx.AuditRevisionID(userAudits.RevisionID),
		auditx.OperationColumn(userAudits.Operation),
		auditx.Key(users.ID, userAudits.UserID),
		auditx.Copy(users.Name, userAudits.Name),
	)
	require.NoError(t, err)
	return spec
}

func openAuditSQLite(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	raw, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	_, err = raw.ExecContext(context.Background(), auditDDL)
	require.NoError(t, err)
	return raw, func() { require.NoError(t, raw.Close()) }
}
