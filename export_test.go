package dbx

import (
	"database/sql"
	schemax "github.com/arcgolabs/dbx/schema"

	collectionx "github.com/arcgolabs/collectionx/list"
)

func TableSpecForTest(schema schemax.Resource) schemax.TableSpec {
	return schema.Spec()
}

func IndexesForTest(schema schemax.Resource) *collectionx.List[schemax.IndexMeta] {
	return schema.Spec().Indexes.Clone()
}

func InferTypeNameForTest(column schemax.ColumnMeta) string {
	return schemax.InferTypeName(column)
}

func ErrorRowForTest(err error) *Row {
	return errorRow(err)
}

func CloseRowsForTest(rows *sql.Rows) error {
	return closeRows(rows)
}

func RowsIterErrorForTest(rows *sql.Rows) error {
	return rowsIterError(rows)
}

func ClonePrimaryKeyMetaForTest(meta schemax.PrimaryKeyMeta) schemax.PrimaryKeyMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func ClonePrimaryKeyStateForTest(state schemax.PrimaryKeyState) schemax.PrimaryKeyState {
	state.Columns = state.Columns.Clone()
	return state
}
