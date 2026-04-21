package querydsl

import (
	"reflect"
	"strings"
)

// Table is a lightweight SQL table reference used by query builders.
type Table struct {
	name       string
	alias      string
	schemaType reflect.Type
	entityType reflect.Type
}

// NewTable creates a table reference from a raw table name.
func NewTable(name string) Table {
	return NewTableRef(name, "", nil, nil)
}

// NamedTable creates a table reference from a raw table name.
func NamedTable(name string) Table {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		panic("dbx/querydsl: named table cannot be empty")
	}
	return NewTable(trimmed)
}

// NewTableRef creates a table reference with optional alias and type metadata.
func NewTableRef(name, alias string, schemaType, entityType reflect.Type) Table {
	return Table{
		name:       strings.TrimSpace(name),
		alias:      strings.TrimSpace(alias),
		schemaType: schemaType,
		entityType: entityType,
	}
}

func TableRef(source TableSource) Table {
	if source == nil {
		return Table{}
	}
	return NewTableRef(source.TableName(), source.TableAlias(), nil, nil)
}

func (t Table) Name() string {
	return t.name
}

func (t Table) TableName() string {
	return t.name
}

func (t Table) Alias() string {
	return t.alias
}

func (t Table) TableAlias() string {
	return t.alias
}

func (t Table) Ref() string {
	if t.alias != "" {
		return t.alias
	}
	return t.name
}

func (t Table) QualifiedName() string {
	if t.alias == "" || t.alias == t.name {
		return t.name
	}
	return t.name + " AS " + t.alias
}

func (t Table) SchemaType() reflect.Type {
	return t.schemaType
}

func (t Table) EntityType() reflect.Type {
	return t.entityType
}

func (t Table) WithEntityType(entityType reflect.Type) Table {
	t.entityType = entityType
	return t
}

func (t Table) WithSchemaType(schemaType reflect.Type) Table {
	t.schemaType = schemaType
	return t
}
