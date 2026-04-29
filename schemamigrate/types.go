package schemamigrate

import (
	"context"
	schemax "github.com/arcgolabs/dbx/schema"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
)

type Resource interface {
	schemax.Resource
}

type Dialect interface {
	dialect.Dialect
	BuildCreateTable(spec schemax.TableSpec) (sqlstmt.Bound, error)
	BuildAddColumn(table string, column schemax.ColumnMeta) (sqlstmt.Bound, error)
	BuildCreateIndex(index schemax.IndexMeta) (sqlstmt.Bound, error)
	BuildAddForeignKey(table string, foreignKey schemax.ForeignKeyMeta) (sqlstmt.Bound, error)
	BuildAddCheck(table string, check schemax.CheckMeta) (sqlstmt.Bound, error)
	InspectTable(ctx context.Context, executor dbx.Executor, table string) (schemax.TableState, error)
	NormalizeType(value string) string
}

func newTableDiff(table string) schemax.TableDiff {
	return schemax.TableDiff{
		Table:              table,
		MissingColumns:     collectionx.NewList[schemax.ColumnMeta](),
		MissingIndexes:     collectionx.NewList[schemax.IndexMeta](),
		MissingForeignKeys: collectionx.NewList[schemax.ForeignKeyMeta](),
		MissingChecks:      collectionx.NewList[schemax.CheckMeta](),
		ColumnDiffs:        collectionx.NewList[schemax.ColumnDiff](),
	}
}
