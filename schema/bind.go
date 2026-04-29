package schema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
)

type schemaBindingState struct {
	binder        schemaBinder
	binderField   reflect.Value
	defTable      schemaTable
	columns       *collectionx.List[ColumnMeta]
	columnsByName *mappingx.Map[string, ColumnMeta]
	relations     *collectionx.List[RelationMeta]
	indexes       *collectionx.List[IndexMeta]
	checks        *collectionx.List[CheckMeta]
	primaryKey    *PrimaryKeyMeta
}

func bindSchema[S any](name, alias string, schema S) (S, error) {
	value := reflect.ValueOf(&schema).Elem()
	if value.Kind() != reflect.Struct {
		return schema, errors.New("dbx: schema must be a struct")
	}

	schemaType := value.Type()
	state := newSchemaBindingState(schemaType, name, alias, value.NumField())
	for i := range value.NumField() {
		if err := state.bindField(schemaType.Field(i), value.Field(i)); err != nil {
			return schema, err
		}
	}

	def, err := state.definition(schemaType)
	if err != nil {
		return schema, err
	}
	state.binderField.Set(reflect.ValueOf(state.binder.bindSchema(def)))
	return schema, nil
}

func newSchemaBindingState(schemaType reflect.Type, name, alias string, fieldCount int) schemaBindingState {
	return schemaBindingState{
		defTable:      newSchemaTable(strings.TrimSpace(name), strings.TrimSpace(alias), schemaType),
		columns:       collectionx.NewListWithCapacity[ColumnMeta](fieldCount),
		columnsByName: mappingx.NewMapWithCapacity[string, ColumnMeta](fieldCount),
		relations:     collectionx.NewListWithCapacity[RelationMeta](fieldCount),
		indexes:       collectionx.NewListWithCapacity[IndexMeta](fieldCount),
		checks:        collectionx.NewListWithCapacity[CheckMeta](fieldCount),
	}
}

func (s *schemaBindingState) bindField(fieldType reflect.StructField, fieldValue reflect.Value) error {
	if !fieldValue.CanSet() {
		return nil
	}
	if s.captureSchemaBinder(fieldValue) {
		return nil
	}
	if handled, err := s.bindColumnField(fieldType, fieldValue); handled || err != nil {
		return err
	}
	if s.bindRelationField(fieldType, fieldValue) {
		return nil
	}
	if handled, err := s.bindConstraintField(fieldType, fieldValue); handled || err != nil {
		return err
	}
	return nil
}

func (s *schemaBindingState) captureSchemaBinder(fieldValue reflect.Value) bool {
	candidate, ok := fieldValue.Interface().(schemaBinder)
	if !ok {
		return false
	}
	s.binder = candidate
	s.binderField = fieldValue
	s.defTable = s.defTable.WithEntityType(candidate.entityType())
	return true
}

func (s *schemaBindingState) bindColumnField(fieldType reflect.StructField, fieldValue reflect.Value) (bool, error) {
	candidate, ok := fieldValue.Interface().(ColumnBinder)
	if !ok {
		return false, nil
	}
	meta, err := resolveColumnMeta(s.defTable, fieldType, fieldValue.Interface())
	if err != nil {
		return true, err
	}
	bound := candidate.BindColumn(ColumnBinding{Meta: meta})
	fieldValue.Set(reflect.ValueOf(bound))
	column := meta
	if accessor, ok := bound.(ColumnAccessor); ok {
		column = accessor.ColumnRef()
	}
	column = cloneColumnMeta(column)
	s.columns.Add(column)
	s.columnsByName.Set(column.Name, column)
	return true, nil
}

func (s *schemaBindingState) bindRelationField(fieldType reflect.StructField, fieldValue reflect.Value) bool {
	candidate, ok := fieldValue.Interface().(RelationBinder)
	if !ok {
		return false
	}
	meta := resolveRelationMeta(s.defTable, fieldType, candidate)
	fieldValue.Set(reflect.ValueOf(candidate.BindRelation(RelationBinding{Meta: meta})))
	s.relations.Add(meta)
	return true
}

func (s *schemaBindingState) bindConstraintField(fieldType reflect.StructField, fieldValue reflect.Value) (bool, error) {
	candidate, ok := fieldValue.Interface().(constraintBinder)
	if !ok {
		return false, nil
	}
	binding, err := resolveConstraintBinding(s.defTable, fieldType, fieldValue.Interface())
	if err != nil {
		return true, err
	}
	fieldValue.Set(reflect.ValueOf(candidate.bindConstraint(binding)))
	s.indexes.Add(binding.indexes...)
	if binding.primaryKey != nil {
		s.primaryKey = new(clonePrimaryKeyMeta(*binding.primaryKey))
	}
	if binding.check != nil {
		s.checks.Add(*binding.check)
	}
	return true, nil
}

func (s *schemaBindingState) definition(schemaType reflect.Type) (schemaDefinition, error) {
	if s.binder == nil {
		return schemaDefinition{}, fmt.Errorf("dbx: schema %s must embed dbx.Schema[T]", schemaType.Name())
	}
	columns := cloneColumnMetas(s.columns)
	columnsByName := s.columnsByName.Clone()
	if columnsByName.Len() == 0 && columns.Len() > 0 {
		columnsByName = indexColumnsByName(columns)
	}
	return schemaDefinition{
		table:         s.defTable,
		columns:       columns,
		columnsByName: columnsByName,
		relations:     s.relations.Clone(),
		indexes:       cloneIndexMetas(s.indexes),
		primaryKey:    clonePrimaryKeyMetaPtr(s.primaryKey),
		checks:        cloneCheckMetas(s.checks),
	}, nil
}

func resolveColumnMeta(def schemaTable, field reflect.StructField, value any) (ColumnMeta, error) {
	name, options := resolveTagNameAndOptions(field)
	meta := ColumnMeta{
		Name:          name,
		Table:         def.Name(),
		Alias:         def.Alias(),
		FieldName:     field.Name,
		GoType:        resolveColumnGoType(value),
		SQLType:       optionValue(options, "type"),
		PrimaryKey:    optionEnabled(options, "pk"),
		AutoIncrement: optionEnabled(options, "auto") || optionEnabled(options, "autoincrement"),
		Nullable:      optionEnabled(options, "nullable") || optionEnabled(options, "null"),
		Unique:        optionEnabled(options, "unique"),
		Indexed:       optionEnabled(options, "index") || optionEnabled(options, "indexed"),
		DefaultValue:  optionValue(options, "default"),
	}

	if refValue := optionValue(options, "ref"); refValue != "" {
		targetTable, targetColumn, ok := splitReference(refValue)
		if ok {
			meta.References = &ForeignKeyRef{
				TargetTable:  targetTable,
				TargetColumn: targetColumn,
				OnDelete:     parseReferentialAction(optionValue(options, "ondelete")),
				OnUpdate:     parseReferentialAction(optionValue(options, "onupdate")),
			}
		}
	}

	return normalizeIDPolicy(meta)
}

func resolveColumnGoType(value any) reflect.Type {
	reporter, ok := value.(ColumnTypeReporter)
	if !ok {
		return nil
	}
	return reporter.ValueType()
}

func resolveRelationMeta(def schemaTable, field reflect.StructField, binder RelationBinder) RelationMeta {
	options := parseTagOptions(field.Tag.Get("rel"))
	name := optionValue(options, "name")
	if name == "" {
		name = toSnakeCase(field.Name)
	}
	return RelationMeta{
		Name:                name,
		FieldName:           field.Name,
		Kind:                binder.RelationKind(),
		SourceTable:         def.Name(),
		SourceAlias:         def.Alias(),
		TargetTable:         optionValue(options, "table"),
		LocalColumn:         optionValue(options, "local"),
		TargetColumn:        optionValue(options, "target"),
		ThroughTable:        optionValue(options, "join"),
		ThroughLocalColumn:  optionValue(options, "join_local"),
		ThroughTargetColumn: optionValue(options, "join_target"),
		TargetType:          binder.TargetType(),
	}
}
