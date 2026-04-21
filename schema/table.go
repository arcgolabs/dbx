package schema

import (
	"reflect"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
)

type SchemaSource[E any] interface {
	Resource
	TableName() string
	TableAlias() string
	schemaRef() schemaDefinition
}

type schemaBinder interface {
	bindSchema(def schemaDefinition) any
	entityType() reflect.Type
}

type schemaDefinition struct {
	table         schemaTable
	columns       collectionx.List[ColumnMeta]
	columnsByName collectionx.Map[string, ColumnMeta]
	relations     collectionx.List[RelationMeta]
	indexes       collectionx.List[IndexMeta]
	primaryKey    *PrimaryKeyMeta
	checks        collectionx.List[CheckMeta]
}

type schemaTable struct {
	name       string
	alias      string
	schemaType reflect.Type
	entityType reflect.Type
}

func newSchemaTable(name, alias string, schemaType reflect.Type) schemaTable {
	return schemaTable{name: name, alias: alias, schemaType: schemaType}
}

func (t schemaTable) WithEntityType(entityType reflect.Type) schemaTable {
	t.entityType = entityType
	return t
}

func (t schemaTable) Name() string { return t.name }

func (t schemaTable) Alias() string { return t.alias }

func (t schemaTable) SchemaType() reflect.Type { return t.schemaType }

func (t schemaTable) EntityType() reflect.Type { return t.entityType }

func (t schemaTable) Ref() string {
	if t.alias != "" {
		return t.alias
	}
	return t.name
}

func (t schemaTable) QualifiedName() string { return t.name }

type Schema[E any] struct {
	def schemaDefinition
}

func (s Schema[E]) bindSchema(def schemaDefinition) any {
	s.def = def
	return s
}

func (Schema[E]) entityType() reflect.Type {
	return reflect.TypeFor[E]()
}

func (s Schema[E]) schemaRef() schemaDefinition {
	return s.def
}

func (s Schema[E]) Name() string {
	return s.def.table.Name()
}

func (s Schema[E]) TableName() string {
	return s.def.table.Name()
}

func (s Schema[E]) Alias() string {
	return s.def.table.Alias()
}

func (s Schema[E]) TableAlias() string {
	return s.def.table.Alias()
}

func (s Schema[E]) Ref() string {
	return s.def.table.Ref()
}

func (s Schema[E]) QualifiedName() string {
	return s.def.table.QualifiedName()
}

func (s Schema[E]) EntityType() reflect.Type {
	return s.def.table.EntityType()
}

func (s Schema[E]) Columns() collectionx.List[ColumnMeta] {
	return cloneColumnMetas(s.def.columns)
}

func (s Schema[E]) Relations() collectionx.List[RelationMeta] {
	return s.def.relations.Clone()
}

func (s Schema[E]) Indexes() collectionx.List[IndexMeta] {
	return cloneIndexMetas(s.def.indexes)
}

func (s Schema[E]) PrimaryKey() (PrimaryKeyMeta, bool) {
	if s.def.primaryKey == nil {
		return PrimaryKeyMeta{}, false
	}
	return clonePrimaryKeyMeta(*s.def.primaryKey), true
}

func (s Schema[E]) Checks() collectionx.List[CheckMeta] {
	return cloneCheckMetas(s.def.checks)
}

func (s Schema[E]) ForeignKeys() collectionx.List[ForeignKeyMeta] {
	items := deriveForeignKeys(s.def)
	return collectionx.MapList[ForeignKeyMeta, ForeignKeyMeta](collectionx.NewListWithCapacity[ForeignKeyMeta](len(items), items...), func(_ int, item ForeignKeyMeta) ForeignKeyMeta {
		return cloneForeignKeyMeta(item)
	})
}

func (s Schema[E]) Spec() TableSpec {
	return buildTableSpec(s.def)
}

func MustSchema[S any](name string, schema S) S {
	bound, err := bindSchema(name, "", schema)
	if err != nil {
		panic(err)
	}
	return bound
}

func Alias[S interface {
	TableName() string
	TableAlias() string
}](schema S, alias string) S {
	if strings.TrimSpace(alias) == "" {
		panic("dbx: alias cannot be empty")
	}
	bound, err := bindSchema(schema.TableName(), alias, schema)
	if err != nil {
		panic(err)
	}
	return bound
}

func cloneColumnMeta(meta ColumnMeta) ColumnMeta {
	if meta.References == nil {
		return meta
	}
	meta.References = new(*meta.References)
	return meta
}

func (d schemaDefinition) columnByName(name string) (ColumnMeta, bool) {
	if d.columnsByName != nil && d.columnsByName.Len() > 0 {
		return d.columnsByName.Get(name)
	}
	return collectionx.FindList[ColumnMeta](d.columns, func(_ int, column ColumnMeta) bool {
		return column.Name == name
	})
}

func cloneColumnMetas(items collectionx.List[ColumnMeta]) collectionx.List[ColumnMeta] {
	return collectionx.MapList[ColumnMeta, ColumnMeta](items, func(_ int, column ColumnMeta) ColumnMeta {
		return cloneColumnMeta(column)
	})
}

func cloneIndexMetas(items collectionx.List[IndexMeta]) collectionx.List[IndexMeta] {
	return collectionx.MapList[IndexMeta, IndexMeta](items, func(_ int, item IndexMeta) IndexMeta {
		return cloneIndexMeta(item)
	})
}

func cloneCheckMetas(items collectionx.List[CheckMeta]) collectionx.List[CheckMeta] {
	return collectionx.MapList[CheckMeta, CheckMeta](items, func(_ int, item CheckMeta) CheckMeta {
		return cloneCheckMeta(item)
	})
}

func indexColumnsByName(columns collectionx.List[ColumnMeta]) collectionx.Map[string, ColumnMeta] {
	return collectionx.AssociateList[ColumnMeta, string, ColumnMeta](columns, func(_ int, column ColumnMeta) (string, ColumnMeta) {
		return column.Name, column
	})
}
