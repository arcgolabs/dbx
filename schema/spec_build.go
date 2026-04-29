package schema

import (
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	setx "github.com/arcgolabs/collectionx/set"
)

func buildTableSpec(def schemaDefinition) TableSpec {
	indexes := deriveIndexes(def)
	foreignKeys := deriveForeignKeys(def)
	checks := deriveChecks(def)
	return TableSpec{
		Name:        def.table.Name(),
		Columns:     cloneColumnMetas(def.columns),
		Indexes:     collectionx.NewListWithCapacity[IndexMeta](len(indexes), indexes...),
		PrimaryKey:  derivePrimaryKey(def),
		ForeignKeys: collectionx.NewListWithCapacity[ForeignKeyMeta](len(foreignKeys), foreignKeys...),
		Checks:      collectionx.NewListWithCapacity[CheckMeta](len(checks), checks...),
	}
}

func deriveIndexes(def schemaDefinition) []IndexMeta {
	indexes := mappingx.NewOrderedMap[string, IndexMeta]()
	def.indexes.Range(func(_ int, index IndexMeta) bool {
		indexes.Set(indexKey(index.Unique, index.Columns), cloneIndexMeta(index))
		return true
	})
	deriveColumnIndexes(def, indexes)
	items := make([]IndexMeta, 0, indexes.Len())
	indexes.Range(func(_ string, value IndexMeta) bool {
		items = append(items, cloneIndexMeta(value))
		return true
	})
	return items
}

func deriveColumnIndexes(def schemaDefinition, indexes *mappingx.OrderedMap[string, IndexMeta]) {
	tableName := def.table.Name()
	def.columns.Range(func(_ int, column ColumnMeta) bool {
		if !shouldDeriveColumnIndex(column) {
			return true
		}
		meta := IndexMeta{
			Name:    indexNameForColumn(tableName, column),
			Table:   tableName,
			Columns: collectionx.NewList[string](column.Name),
			Unique:  column.Unique,
		}
		indexes.Set(indexKey(meta.Unique, meta.Columns), meta)
		return true
	})
}

func shouldDeriveColumnIndex(column ColumnMeta) bool {
	return !column.PrimaryKey && (column.Unique || column.Indexed)
}

func indexNameForColumn(table string, column ColumnMeta) string {
	prefix := "idx"
	if column.Unique {
		prefix = "ux"
	}
	return prefix + "_" + table + "_" + column.Name
}

func derivePrimaryKey(def schemaDefinition) *PrimaryKeyMeta {
	tableName := def.table.Name()
	if def.primaryKey != nil {
		copyPrimary := clonePrimaryKeyMeta(*def.primaryKey)
		if copyPrimary.Name == "" {
			copyPrimary.Name = "pk_" + tableName
		}
		if copyPrimary.Table == "" {
			copyPrimary.Table = tableName
		}
		return &copyPrimary
	}

	columns := collectionx.FilterMapList[ColumnMeta, string](def.columns, func(_ int, column ColumnMeta) (string, bool) {
		return column.Name, column.PrimaryKey
	})
	if columns.Len() == 0 {
		return nil
	}
	return &PrimaryKeyMeta{
		Name:    "pk_" + tableName,
		Table:   tableName,
		Columns: columns,
	}
}

func deriveForeignKeys(def schemaDefinition) []ForeignKeyMeta {
	foreignKeys := mappingx.NewOrderedMap[string, ForeignKeyMeta]()
	explicitColumns := setx.NewSet[string]()
	deriveExplicitForeignKeys(def, foreignKeys, explicitColumns)
	deriveRelationForeignKeys(def, foreignKeys, explicitColumns)
	items := make([]ForeignKeyMeta, 0, foreignKeys.Len())
	foreignKeys.Range(func(_ string, value ForeignKeyMeta) bool {
		items = append(items, cloneForeignKeyMeta(value))
		return true
	})
	return items
}

func deriveExplicitForeignKeys(def schemaDefinition, foreignKeys *mappingx.OrderedMap[string, ForeignKeyMeta], explicitColumns *setx.Set[string]) {
	tableName := def.table.Name()
	def.columns.Range(func(_ int, column ColumnMeta) bool {
		if column.References == nil {
			return true
		}
		explicitColumns.Add(column.Name)
		meta := ForeignKeyMeta{
			Name:          "fk_" + tableName + "_" + column.Name,
			Table:         tableName,
			Columns:       collectionx.NewList[string](column.Name),
			TargetTable:   column.References.TargetTable,
			TargetColumns: collectionx.NewList[string](column.References.TargetColumn),
			OnDelete:      column.References.OnDelete,
			OnUpdate:      column.References.OnUpdate,
		}
		foreignKeys.Set(foreignKeyKey(meta), meta)
		return true
	})
}

func deriveRelationForeignKeys(def schemaDefinition, foreignKeys *mappingx.OrderedMap[string, ForeignKeyMeta], explicitColumns *setx.Set[string]) {
	tableName := def.table.Name()
	def.relations.Range(func(_ int, relation RelationMeta) bool {
		if !shouldDeriveRelationForeignKey(def, relation, explicitColumns) {
			return true
		}
		meta := ForeignKeyMeta{
			Name:          "fk_" + tableName + "_" + relation.LocalColumn,
			Table:         tableName,
			Columns:       collectionx.NewList[string](relation.LocalColumn),
			TargetTable:   relation.TargetTable,
			TargetColumns: collectionx.NewList[string](relation.TargetColumn),
		}
		key := foreignKeyKey(meta)
		if _, exists := foreignKeys.Get(key); !exists {
			foreignKeys.Set(key, meta)
		}
		return true
	})
}

func shouldDeriveRelationForeignKey(def schemaDefinition, relation RelationMeta, explicitColumns *setx.Set[string]) bool {
	if relation.Kind != RelationBelongsTo {
		return false
	}
	if relation.LocalColumn == "" || relation.TargetColumn == "" || relation.TargetTable == "" {
		return false
	}
	if explicitColumns.Contains(relation.LocalColumn) {
		return false
	}
	_, ok := def.columnByName(relation.LocalColumn)
	return ok
}

func deriveChecks(def schemaDefinition) []CheckMeta {
	return collectionx.MapList[CheckMeta, CheckMeta](def.checks, func(_ int, check CheckMeta) CheckMeta {
		return cloneCheckMeta(check)
	}).Values()
}

func indexKey(unique bool, columns *collectionx.List[string]) string {
	prefix := "idx:"
	if unique {
		prefix = "ux:"
	}
	return prefix + columnsKey(columns)
}

func foreignKeyKey(meta ForeignKeyMeta) string {
	return columnsKey(meta.Columns) + "->" + meta.TargetTable + ":" + columnsKey(meta.TargetColumns) + ":" + string(normalizeReferentialAction(meta.OnDelete)) + ":" + string(normalizeReferentialAction(meta.OnUpdate))
}

func columnsKey(columns *collectionx.List[string]) string {
	return columns.Join(",")
}

func normalizeReferentialAction(action ReferentialAction) ReferentialAction {
	if strings.TrimSpace(string(action)) == "" {
		return ReferentialNoAction
	}
	return action
}
