package schemamigrate

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	atlasmigrate "ariga.io/atlas/sql/migrate"
	atlasschema "ariga.io/atlas/sql/schema"
	"github.com/arcgolabs/collectionx"
)

func compileAtlasSchema(dialectName string, driver atlasmigrate.Driver, schemaName string, schemas []Resource) *atlasCompiledSchema {
	compiled := newAtlasCompiledSchema(schemaName, schemas)
	for _, resource := range schemas {
		compileAtlasResource(compiled, dialectName, driver, resource)
	}
	attachCompiledTableConstraints(compiled)
	return compiled
}

func newAtlasCompiledSchema(schemaName string, schemas []Resource) *atlasCompiledSchema {
	atlasSchema := atlasschema.New(schemaName)
	order := collectionx.NewListWithCapacity[string](len(schemas))
	for _, schema := range schemas {
		order.Add(schema.TableName())
	}
	return &atlasCompiledSchema{
		schema:    atlasSchema,
		tables:    collectionx.NewMapWithCapacity[string, *atlasCompiledTable](len(schemas)),
		externals: collectionx.NewMap[string, *atlasschema.Table](),
		order:     order,
	}
}

func compileAtlasResource(compiled *atlasCompiledSchema, dialectName string, driver atlasmigrate.Driver, resource Resource) {
	spec := resource.Spec()
	table := atlasschema.NewTable(spec.Name).SetSchema(compiled.schema)
	compiledTable := newAtlasCompiledTable(spec, table)
	compileAtlasTableColumns(compiledTable, dialectName, driver)
	compileAtlasTableMetadata(compiledTable)
	compiled.schema.AddTables(table)
	compiled.tables.Set(spec.Name, compiledTable)
}

func newAtlasCompiledTable(spec schemax.TableSpec, table *atlasschema.Table) *atlasCompiledTable {
	return &atlasCompiledTable{
		spec:              spec,
		table:             table,
		columnsByName:     collectionx.NewMapWithCapacity[string, schemax.ColumnMeta](spec.Columns.Len()),
		indexesByName:     collectionx.NewMapWithCapacity[string, schemax.IndexMeta](spec.Indexes.Len()),
		indexesByKey:      collectionx.NewMapWithCapacity[string, schemax.IndexMeta](spec.Indexes.Len()),
		foreignKeysByName: collectionx.NewMapWithCapacity[string, schemax.ForeignKeyMeta](spec.ForeignKeys.Len()),
		foreignKeysByKey:  collectionx.NewMapWithCapacity[string, schemax.ForeignKeyMeta](spec.ForeignKeys.Len()),
		checksByName:      collectionx.NewMapWithCapacity[string, schemax.CheckMeta](spec.Checks.Len()),
		checksByExpr:      collectionx.NewMapWithCapacity[string, schemax.CheckMeta](spec.Checks.Len()),
	}
}

func compileAtlasTableColumns(compiledTable *atlasCompiledTable, dialectName string, driver atlasmigrate.Driver) {
	compiledTable.spec.Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		atlasColumn := compileAtlasColumn(dialectName, driver, column)
		compiledTable.table.AddColumns(atlasColumn)
		compiledTable.columnsByName.Set(column.Name, column)
		return true
	})
}

func compileAtlasTableMetadata(compiledTable *atlasCompiledTable) {
	compiledTable.spec.Indexes.Range(func(_ int, index schemax.IndexMeta) bool {
		compiledTable.indexesByName.Set(index.Name, index)
		compiledTable.indexesByKey.Set(indexKey(index.Unique, index.Columns), index)
		return true
	})
	compiledTable.spec.ForeignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		compiledTable.foreignKeysByName.Set(foreignKey.Name, foreignKey)
		compiledTable.foreignKeysByKey.Set(foreignKeyKey(foreignKey), foreignKey)
		return true
	})
	compiledTable.spec.Checks.Range(func(_ int, check schemax.CheckMeta) bool {
		compiledTable.checksByName.Set(check.Name, check)
		compiledTable.checksByExpr.Set(checkKey(check.Expression), check)
		return true
	})
}

func attachCompiledTableConstraints(compiled *atlasCompiledSchema) {
	compiled.tables.Range(func(_ string, table *atlasCompiledTable) bool {
		attachCompiledPrimaryKey(table)
		attachCompiledIndexes(table)
		attachCompiledForeignKeys(compiled, table)
		attachCompiledChecks(table)
		return true
	})
}

func attachCompiledPrimaryKey(table *atlasCompiledTable) {
	if table.spec.PrimaryKey == nil {
		return
	}
	if primaryKey := atlasPrimaryKeyForSpec(table.table, *table.spec.PrimaryKey); primaryKey != nil {
		table.table.SetPrimaryKey(primaryKey)
	}
}

func attachCompiledIndexes(table *atlasCompiledTable) {
	table.spec.Indexes.Range(func(_ int, index schemax.IndexMeta) bool {
		if atlasIndex := atlasIndexForSpec(table.table, index); atlasIndex != nil {
			table.table.AddIndexes(atlasIndex)
		}
		return true
	})
}

func attachCompiledForeignKeys(compiled *atlasCompiledSchema, table *atlasCompiledTable) {
	table.spec.ForeignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		if atlasForeignKey := compiled.atlasForeignKeyForSpec(table.table, foreignKey); atlasForeignKey != nil {
			table.table.AddForeignKeys(atlasForeignKey)
		}
		return true
	})
}

func attachCompiledChecks(table *atlasCompiledTable) {
	table.spec.Checks.Range(func(_ int, check schemax.CheckMeta) bool {
		table.table.AddChecks(atlasschema.NewCheck().SetName(check.Name).SetExpr(check.Expression))
		return true
	})
}

func compileAtlasColumn(dialectName string, driver atlasmigrate.Driver, column schemax.ColumnMeta) *atlasschema.Column {
	rawType := atlasColumnRawType(dialectName, column)
	atlasColumn := atlasschema.NewColumn(column.Name)
	atlasColumn.Type = &atlasschema.ColumnType{
		Type: atlasColumnType(driver, rawType, column),
		Raw:  rawType,
		Null: column.Nullable,
	}
	atlasColumn.SetNull(column.Nullable)
	if column.DefaultValue != "" {
		atlasColumn.SetDefault(&atlasschema.RawExpr{X: column.DefaultValue})
	}
	if column.AutoIncrement {
		atlasAddAutoIncrementAttr(dialectName, atlasColumn)
	}
	return atlasColumn
}

func atlasColumnRawType(dialectName string, column schemax.ColumnMeta) string {
	rawType := strings.TrimSpace(column.SQLType)
	if rawType == "" {
		rawType = InferTypeName(column)
	}
	if strings.EqualFold(strings.TrimSpace(dialectName), "sqlite") && column.AutoIncrement {
		return "integer"
	}
	return rawType
}

func atlasColumnType(driver atlasmigrate.Driver, rawType string, column schemax.ColumnMeta) atlasschema.Type {
	if parser, ok := driver.(atlasschema.TypeParser); ok && rawType != "" {
		if parsed, err := parser.ParseType(rawType); err == nil && parsed != nil {
			return parsed
		}
	}
	return atlasFallbackType(rawType, column)
}

func (c *atlasCompiledSchema) atlasForeignKeyForSpec(table *atlasschema.Table, foreignKey schemax.ForeignKeyMeta) *atlasschema.ForeignKey {
	columns := atlasColumnsByName(table, foreignKey.Columns)
	if len(columns) == 0 {
		return nil
	}
	refTable := c.referenceTable(table.Schema, foreignKey.TargetTable, foreignKey.TargetColumns)
	refColumns := atlasColumnsByName(refTable, foreignKey.TargetColumns)
	if len(refColumns) == 0 {
		return nil
	}
	return atlasschema.NewForeignKey(foreignKey.Name).
		SetTable(table).
		AddColumns(columns...).
		SetRefTable(refTable).
		AddRefColumns(refColumns...).
		SetOnDelete(atlasReferenceAction(foreignKey.OnDelete)).
		SetOnUpdate(atlasReferenceAction(foreignKey.OnUpdate))
}

func (c *atlasCompiledSchema) referenceTable(schema *atlasschema.Schema, tableName string, targetColumns collectionx.List[string]) *atlasschema.Table {
	if compiled, ok := c.tables.Get(tableName); ok {
		return compiled.table
	}
	if external, ok := c.externals.Get(tableName); ok {
		return external
	}
	external := atlasschema.NewTable(tableName).SetSchema(schema)
	targetColumns.Range(func(_ int, column string) bool {
		external.AddColumns(atlasschema.NewColumn(column))
		return true
	})
	c.externals.Set(tableName, external)
	return external
}

func atlasReferenceAction(action schemax.ReferentialAction) atlasschema.ReferenceOption {
	switch normalized := normalizeReferentialAction(action); normalized {
	case schemax.ReferentialCascade:
		return atlasschema.Cascade
	case schemax.ReferentialRestrict:
		return atlasschema.Restrict
	case schemax.ReferentialSetNull:
		return atlasschema.SetNull
	case schemax.ReferentialSetDefault:
		return atlasschema.SetDefault
	case schemax.ReferentialNoAction:
		return atlasschema.NoAction
	default:
		return atlasschema.NoAction
	}
}
