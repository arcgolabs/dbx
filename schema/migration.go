package schema

import (
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/sqlstmt"
)

type TableSpec struct {
	Name        string
	Columns     collectionx.List[ColumnMeta]
	Indexes     collectionx.List[IndexMeta]
	PrimaryKey  *PrimaryKeyMeta
	ForeignKeys collectionx.List[ForeignKeyMeta]
	Checks      collectionx.List[CheckMeta]
}

type TableState struct {
	Exists      bool
	Name        string
	Columns     collectionx.List[ColumnState]
	Indexes     collectionx.List[IndexState]
	PrimaryKey  *PrimaryKeyState
	ForeignKeys collectionx.List[ForeignKeyState]
	Checks      collectionx.List[CheckState]
}

type ColumnState struct {
	Name          string
	Type          string
	Nullable      bool
	PrimaryKey    bool
	AutoIncrement bool
	DefaultValue  string
}

type IndexState struct {
	Name    string
	Columns collectionx.List[string]
	Unique  bool
}

type PrimaryKeyState struct {
	Name    string
	Columns collectionx.List[string]
}

type ForeignKeyState struct {
	Name          string
	Columns       collectionx.List[string]
	TargetTable   string
	TargetColumns collectionx.List[string]
	OnDelete      ReferentialAction
	OnUpdate      ReferentialAction
}

type CheckState struct {
	Name       string
	Expression string
}

type ValidationReport struct {
	Tables   collectionx.List[TableDiff]
	Backend  ValidationBackend
	Complete bool
	Warnings collectionx.List[string]
}

type ValidationBackend string

const (
	ValidationBackendAtlas  ValidationBackend = "atlas"
	ValidationBackendLegacy ValidationBackend = "legacy"
)

type TableDiff struct {
	Table              string
	MissingTable       bool
	MissingColumns     collectionx.List[ColumnMeta]
	MissingIndexes     collectionx.List[IndexMeta]
	MissingForeignKeys collectionx.List[ForeignKeyMeta]
	MissingChecks      collectionx.List[CheckMeta]
	PrimaryKeyDiff     *PrimaryKeyDiff
	ColumnDiffs        collectionx.List[ColumnDiff]
}

type PrimaryKeyDiff struct {
	Expected *PrimaryKeyMeta
	Actual   *PrimaryKeyState
	Issues   collectionx.List[string]
}

type ColumnDiff struct {
	Column ColumnMeta
	Issues collectionx.List[string]
}

type MigrationActionKind string

const (
	MigrationActionCreateTable   MigrationActionKind = "create_table"
	MigrationActionAddColumn     MigrationActionKind = "add_column"
	MigrationActionCreateIndex   MigrationActionKind = "create_index"
	MigrationActionAddForeignKey MigrationActionKind = "add_foreign_key"
	MigrationActionAddCheck      MigrationActionKind = "add_check"
	MigrationActionManual        MigrationActionKind = "manual"
)

type MigrationAction struct {
	Kind       MigrationActionKind
	Table      string
	Summary    string
	Statement  sqlstmt.Bound
	Executable bool
}

type MigrationPlan struct {
	Actions collectionx.List[MigrationAction]
	Report  ValidationReport
}

func (a MigrationAction) HasStatement() bool {
	return strings.TrimSpace(a.Statement.SQL) != ""
}

func (a MigrationAction) SQLPreview() string {
	return strings.TrimSpace(a.Statement.SQL)
}

func (p MigrationPlan) Statements() collectionx.List[sqlstmt.Bound] {
	statements := collectionx.NewListWithCapacity[sqlstmt.Bound](p.Actions.Len())
	p.Actions.Range(func(_ int, action MigrationAction) bool {
		if action.HasStatement() {
			statements.Add(action.Statement)
		}
		return true
	})
	return statements
}

func (p MigrationPlan) SQLPreview() collectionx.List[string] {
	preview := collectionx.NewListWithCapacity[string](p.Actions.Len())
	p.Actions.Range(func(_ int, action MigrationAction) bool {
		if action.HasStatement() {
			preview.Add(action.SQLPreview())
		}
		return true
	})
	return preview
}

type SchemaDriftError struct {
	Report ValidationReport
}

func (e SchemaDriftError) Error() string {
	tables := collectionx.NewListWithCapacity[string](e.Report.Tables.Len())
	e.Report.Tables.Range(func(_ int, table TableDiff) bool {
		if !table.Empty() {
			tables.Add(table.Table)
		}
		return true
	})
	if tables.Len() == 0 {
		return "dbx: schema drift detected"
	}
	return "dbx: schema drift detected for tables: " + tables.Join(", ")
}

func (r ValidationReport) Valid() bool {
	valid := true
	r.Tables.Range(func(_ int, table TableDiff) bool {
		if table.Empty() {
			return true
		}
		valid = false
		return false
	})
	return valid
}

func (r ValidationReport) HasWarnings() bool {
	return r.Warnings.Len() > 0
}

func (r ValidationReport) IsComplete() bool {
	return r.Complete
}

func (r ValidationReport) WithWarning(message string) ValidationReport {
	if strings.TrimSpace(message) == "" {
		return r
	}
	next := r
	next.Warnings = r.Warnings.Clone()
	next.Warnings.Add(message)
	return next
}

func (t TableDiff) Empty() bool {
	return !t.MissingTable &&
		t.MissingColumns.Len() == 0 &&
		t.MissingIndexes.Len() == 0 &&
		t.MissingForeignKeys.Len() == 0 &&
		t.MissingChecks.Len() == 0 &&
		t.PrimaryKeyDiff == nil &&
		t.ColumnDiffs.Len() == 0
}

func (p MigrationPlan) ExecutableActions() collectionx.List[MigrationAction] {
	actions := collectionx.NewListWithCapacity[MigrationAction](p.Actions.Len())
	p.Actions.Range(func(_ int, action MigrationAction) bool {
		if action.Executable {
			actions.Add(action)
		}
		return true
	})
	return actions
}

func (p MigrationPlan) HasManualActions() bool {
	manual := false
	p.Actions.Range(func(_ int, action MigrationAction) bool {
		if action.Executable {
			return true
		}
		manual = true
		return false
	})
	return manual
}
