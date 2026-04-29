package schema

import (
	collectionx "github.com/arcgolabs/collectionx/list"
)

func (s Schema[E]) PrimaryColumn() (ColumnMeta, bool) {
	column, ok := collectionx.FindList[ColumnMeta](s.def.columns, func(_ int, column ColumnMeta) bool {
		return column.PrimaryKey
	})
	if !ok {
		return ColumnMeta{}, false
	}
	return cloneColumnMeta(column), true
}

func (s Schema[E]) ColumnByName(name string) (ColumnMeta, bool) {
	column, ok := s.def.columnByName(name)
	if !ok {
		return ColumnMeta{}, false
	}
	return cloneColumnMeta(column), true
}
