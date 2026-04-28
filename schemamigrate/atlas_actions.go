package schemamigrate

import (
	"context"
	"errors"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
	"strings"

	atlasmigrate "ariga.io/atlas/sql/migrate"
	atlasschema "ariga.io/atlas/sql/schema"
	"github.com/arcgolabs/collectionx"
)

func atlasSplitChanges(changes []atlasschema.Change) ([]atlasschema.Change, []schemax.MigrationAction) {
	result := atlasSplitResult{
		safeChanges:   make([]atlasschema.Change, 0, len(changes)),
		manualActions: make([]schemax.MigrationAction, 0, len(changes)),
	}
	collectionx.NewListWithCapacity[atlasschema.Change](len(changes), changes...).Range(func(_ int, change atlasschema.Change) bool {
		currentSafe, currentManual := atlasClassifyChange(change)
		result.safeChanges = append(result.safeChanges, currentSafe...)
		result.manualActions = append(result.manualActions, currentManual...)
		return true
	})
	return result.safeChanges, result.manualActions
}

func atlasClassifyChange(change atlasschema.Change) ([]atlasschema.Change, []schemax.MigrationAction) {
	switch current := change.(type) {
	case *atlasschema.AddTable:
		return []atlasschema.Change{current}, nil
	case *atlasschema.ModifyTable:
		return atlasClassifyModifyTableChange(current)
	default:
		return nil, []schemax.MigrationAction{atlasManualAction(atlasChangeTableName(change), change)}
	}
}

func atlasClassifyModifyTableChange(change *atlasschema.ModifyTable) ([]atlasschema.Change, []schemax.MigrationAction) {
	changes := collectionx.NewListWithCapacity[atlasschema.Change](len(change.Changes), change.Changes...)
	safeChanges := collectionx.FilterMapList[atlasschema.Change, atlasschema.Change](changes, func(_ int, tableChange atlasschema.Change) (atlasschema.Change, bool) {
		if !atlasIsExecutableTableChange(tableChange) {
			return nil, false
		}
		return &atlasschema.ModifyTable{T: change.T, Changes: []atlasschema.Change{tableChange}}, true
	})
	manualActions := collectionx.FilterMapList[atlasschema.Change, schemax.MigrationAction](changes, func(_ int, tableChange atlasschema.Change) (schemax.MigrationAction, bool) {
		if atlasIsExecutableTableChange(tableChange) {
			return schemax.MigrationAction{}, false
		}
		return atlasManualAction(change.T.Name, tableChange), true
	})
	return safeChanges.Values(), manualActions.Values()
}

func atlasIsExecutableTableChange(change atlasschema.Change) bool {
	switch change.(type) {
	case *atlasschema.AddColumn, *atlasschema.AddIndex, *atlasschema.AddForeignKey, *atlasschema.AddCheck:
		return true
	default:
		return false
	}
}

func atlasPlanActions(ctx context.Context, driver atlasmigrate.Driver, changes []atlasschema.Change) ([]schemax.MigrationAction, error) {
	actions, err := collectionx.ReduceErrList[atlasschema.Change, collectionx.List[schemax.MigrationAction]](
		collectionx.NewListWithCapacity[atlasschema.Change](len(changes), changes...),
		collectionx.NewListWithCapacity[schemax.MigrationAction](len(changes)),
		func(result collectionx.List[schemax.MigrationAction], _ int, change atlasschema.Change) (collectionx.List[schemax.MigrationAction], error) {
			plannedActions, planErr := atlasPlanChangeActions(ctx, driver, change)
			if planErr != nil {
				return nil, planErr
			}
			result.Merge(collectionx.NewListWithCapacity[schemax.MigrationAction](len(plannedActions), plannedActions...))
			return result, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return actions.Values(), nil
}

func atlasPlanChangeActions(ctx context.Context, driver atlasmigrate.Driver, change atlasschema.Change) ([]schemax.MigrationAction, error) {
	plan, err := driver.PlanChanges(ctx, "dbx_schema_plan", []atlasschema.Change{change})
	if err != nil {
		if errors.Is(err, atlasmigrate.ErrNoPlan) {
			return nil, nil
		}
		return nil, wrapMigrateError("plan atlas schema changes", err)
	}
	return collectionx.MapList[*atlasmigrate.Change, schemax.MigrationAction](collectionx.NewListWithCapacity[*atlasmigrate.Change](len(plan.Changes), plan.Changes...), func(_ int, planned *atlasmigrate.Change) schemax.MigrationAction {
		return atlasPlannedAction(change, planned)
	}).Values(), nil
}

func atlasPlannedAction(change atlasschema.Change, planned *atlasmigrate.Change) schemax.MigrationAction {
	summary := strings.TrimSpace(planned.Comment)
	if summary == "" {
		summary = atlasActionSummary(change)
	}
	return schemax.MigrationAction{
		Kind:       atlasActionKind(change),
		Table:      atlasChangeTableName(change),
		Summary:    summary,
		Statement:  sqlstmt.Bound{SQL: planned.Cmd, Args: collectionx.NewList[any](planned.Args...)},
		Executable: true,
	}
}

func atlasActionKind(change atlasschema.Change) schemax.MigrationActionKind {
	switch current := change.(type) {
	case *atlasschema.AddTable:
		return schemax.MigrationActionCreateTable
	case *atlasschema.ModifyTable:
		if len(current.Changes) != 1 {
			return schemax.MigrationActionManual
		}
		return atlasModifyTableActionKind(current.Changes[0])
	default:
		return schemax.MigrationActionManual
	}
}

func atlasModifyTableActionKind(change atlasschema.Change) schemax.MigrationActionKind {
	switch change.(type) {
	case *atlasschema.AddColumn:
		return schemax.MigrationActionAddColumn
	case *atlasschema.AddIndex:
		return schemax.MigrationActionCreateIndex
	case *atlasschema.AddForeignKey:
		return schemax.MigrationActionAddForeignKey
	case *atlasschema.AddCheck:
		return schemax.MigrationActionAddCheck
	default:
		return schemax.MigrationActionManual
	}
}

func atlasActionSummary(change atlasschema.Change) string {
	switch current := change.(type) {
	case *atlasschema.AddTable:
		return "create table " + current.T.Name
	case *atlasschema.ModifyTable:
		return atlasModifyTableActionSummary(current)
	default:
		return atlasManualSummary(change)
	}
}

func atlasModifyTableActionSummary(change *atlasschema.ModifyTable) string {
	if len(change.Changes) != 1 {
		return "modify table " + change.T.Name
	}
	switch current := change.Changes[0].(type) {
	case *atlasschema.AddColumn:
		return "add column " + current.C.Name
	case *atlasschema.AddIndex:
		return "create index " + current.I.Name
	case *atlasschema.AddForeignKey:
		return "add foreign key " + current.F.Symbol
	case *atlasschema.AddCheck:
		return "add check " + current.C.Name
	default:
		return atlasManualSummary(current)
	}
}

func atlasManualAction(table string, change atlasschema.Change) schemax.MigrationAction {
	return schemax.MigrationAction{Kind: schemax.MigrationActionManual, Table: table, Summary: atlasManualSummary(change)}
}

type atlasManualSummaryHandler func(atlasschema.Change) (string, bool)

var atlasManualSummaryHandlers = []atlasManualSummaryHandler{
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.ModifyColumn)
		if !ok {
			return "", false
		}
		return "manual column migration required for " + current.To.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.RenameColumn)
		if !ok {
			return "", false
		}
		return "manual column rename migration required for " + current.From.Name + " -> " + current.To.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.DropColumn)
		if !ok {
			return "", false
		}
		return "manual column removal migration required for " + current.C.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.ModifyIndex)
		if !ok {
			return "", false
		}
		return "manual index migration required for " + current.To.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.RenameIndex)
		if !ok {
			return "", false
		}
		return "manual index rename migration required for " + current.From.Name + " -> " + current.To.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.DropIndex)
		if !ok {
			return "", false
		}
		return "manual index removal migration required for " + current.I.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		switch change.(type) {
		case *atlasschema.AddPrimaryKey, *atlasschema.ModifyPrimaryKey, *atlasschema.DropPrimaryKey:
			return "manual primary key migration required", true
		default:
			return "", false
		}
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.ModifyForeignKey)
		if !ok {
			return "", false
		}
		return "manual foreign key migration required for " + current.To.Symbol, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.DropForeignKey)
		if !ok {
			return "", false
		}
		return "manual foreign key removal migration required for " + current.F.Symbol, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.ModifyCheck)
		if !ok {
			return "", false
		}
		return "manual check migration required for " + current.To.Name, true
	},
	func(change atlasschema.Change) (string, bool) {
		current, ok := change.(*atlasschema.DropCheck)
		if !ok {
			return "", false
		}
		return "manual check removal migration required for " + current.C.Name, true
	},
}

func atlasManualSummary(change atlasschema.Change) string {
	for _, handler := range atlasManualSummaryHandlers {
		if summary, ok := handler(change); ok {
			return summary
		}
	}
	return "manual schema migration required"
}

func atlasChangeTableName(change atlasschema.Change) string {
	switch current := change.(type) {
	case *atlasschema.AddTable:
		return current.T.Name
	case *atlasschema.ModifyTable:
		return current.T.Name
	case *atlasschema.DropTable:
		return current.T.Name
	default:
		return ""
	}
}

type atlasSplitResult struct {
	safeChanges   []atlasschema.Change
	manualActions []schemax.MigrationAction
}
