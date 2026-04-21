package querydsl

import (
	"github.com/DaiYuANg/arcgo/collectionx"
	schemax "github.com/arcgolabs/dbx/schema"
)

type schemaColumnSelectItem struct {
	meta schemax.ColumnMeta
}

// AllColumns returns a select list for every column exposed by a schema.
func AllColumns(schema schemax.Resource) collectionx.List[SelectItem] {
	return collectionx.MapList[schemax.ColumnMeta, SelectItem](schema.Spec().Columns, func(_ int, column schemax.ColumnMeta) SelectItem {
		return schemaColumnSelectItem{meta: schemax.CloneColumnMeta(column)}
	})
}

func (i schemaColumnSelectItem) QuerySelectItem() {}

func (i schemaColumnSelectItem) RenderSelectItem(state *State) error {
	state.RenderColumn(i.meta)
	return nil
}
