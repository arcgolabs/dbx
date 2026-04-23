package mapper

import (
	"context"
	"errors"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/querydsl"
)

func (m Mapper[E]) InsertAssignments(session any, schema schemax.Resource, entity *E) (collectionx.List[querydsl.Assignment], error) {
	if session == nil {
		return nil, ErrNilSession
	}
	carrier, ok := session.(interface{ IDGenerator() idgen.Generator })
	if !ok {
		return nil, errors.New("dbx: session does not expose id generator")
	}
	return m.InsertAssignmentsWithID(context.Background(), schema, entity, carrier.IDGenerator())
}

func (m Mapper[E]) InsertAssignmentsWithID(ctx context.Context, schema schemax.Resource, entity *E, generator idgen.Generator) (collectionx.List[querydsl.Assignment], error) {
	return m.entityAssignments(ctx, schema, entity, generator, func(column schemax.ColumnMeta, field MappedField) bool {
		if !field.Insertable {
			return false
		}
		if !column.PrimaryKey {
			return true
		}
		return column.IDStrategy != idgen.StrategyDBAuto && !column.AutoIncrement
	})
}

func (m Mapper[E]) UpdateAssignments(schema schemax.Resource, entity *E) (collectionx.List[querydsl.Assignment], error) {
	return m.entityAssignments(context.Background(), schema, entity, nil, func(column schemax.ColumnMeta, field MappedField) bool {
		if !field.Updatable {
			return false
		}
		return !column.PrimaryKey && !column.AutoIncrement
	})
}

func (m Mapper[E]) PrimaryPredicate(schema schemax.Resource, entity *E) (querydsl.Predicate, error) {
	value, err := m.entityValue(entity)
	if err != nil {
		return nil, err
	}

	var predicate querydsl.Predicate
	var resultErr error
	schema.Spec().Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		if !column.PrimaryKey {
			return true
		}
		predicate, err = m.primaryColumnPredicate(value, column)
		if err != nil {
			resultErr = err
			return false
		}
		return false
	})
	if resultErr != nil {
		return nil, resultErr
	}
	if predicate != nil {
		return predicate, nil
	}

	return nil, ErrNoPrimaryKey
}

func (m Mapper[E]) primaryColumnPredicate(value reflect.Value, column schemax.ColumnMeta) (querydsl.Predicate, error) {
	field, ok := m.byColumn.Get(column.Name)
	if !ok {
		return nil, &PrimaryKeyUnmappedError{Column: column.Name}
	}
	fieldValue, err := fieldValueForRead(value, field)
	if err != nil {
		return nil, err
	}
	boundValue, err := boundFieldValue(field, fieldValue)
	if err != nil {
		return nil, err
	}
	return metadataComparisonPredicate{
		left:  column,
		op:    querydsl.OpEq,
		right: boundValue,
	}, nil
}

func (m Mapper[E]) entityAssignments(ctx context.Context, schema schemax.Resource, entity *E, generator idgen.Generator, include func(column schemax.ColumnMeta, field MappedField) bool) (collectionx.List[querydsl.Assignment], error) {
	value, err := m.entityValue(entity)
	if err != nil {
		return nil, err
	}

	columns := schema.Spec().Columns
	assignments := collectionx.NewListWithCapacity[querydsl.Assignment](columns.Len())
	var resultErr error
	columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		field, ok := m.byColumn.Get(column.Name)
		if !ok || !include(column, field) {
			return true
		}
		assignment, ok, err := m.buildAssignment(ctx, value, column, field, generator)
		if err != nil {
			resultErr = err
			return false
		}
		if ok {
			assignments.Add(assignment)
		}
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}

	return assignments, nil
}

func shouldGenerateID(column schemax.ColumnMeta) bool {
	return column.IDStrategy == idgen.StrategySnowflake ||
		column.IDStrategy == idgen.StrategyUUID ||
		column.IDStrategy == idgen.StrategyULID ||
		column.IDStrategy == idgen.StrategyKSUID
}

func (m Mapper[E]) ensureGeneratedID(ctx context.Context, root reflect.Value, field MappedField, column schemax.ColumnMeta, generator idgen.Generator) (reflect.Value, bool, error) {
	fieldValue, err := fieldValueForRead(root, field)
	if err != nil {
		return reflect.Value{}, false, err
	}
	if fieldValue.IsValid() && !fieldValue.IsZero() {
		return fieldValue, false, nil
	}
	if generator == nil {
		return reflect.Value{}, false, fmt.Errorf("dbx: id generator is nil for column %s", column.Name)
	}
	generated, err := generator.GenerateID(ctx, idgen.Request{
		Strategy:    column.IDStrategy,
		UUIDVersion: column.UUIDVersion,
	})
	if err != nil {
		return reflect.Value{}, false, fmt.Errorf("dbx: generate id for column %s: %w", column.Name, err)
	}

	targetField, err := ensureFieldValue(root, field)
	if err != nil {
		return reflect.Value{}, false, err
	}
	if !targetField.CanSet() {
		return reflect.Value{}, false, fmt.Errorf("dbx: cannot set generated id for column %s", column.Name)
	}

	assignedField, assigned, setErr := setGeneratedValue(targetField, generated, column)
	if setErr != nil {
		return reflect.Value{}, false, setErr
	}
	if assigned {
		return assignedField, true, nil
	}
	return reflect.Value{}, false, fmt.Errorf("dbx: generated id type %s cannot be assigned to %s for column %s", reflect.TypeOf(generated), targetField.Type(), column.Name)
}

func setGeneratedValue(targetField reflect.Value, generated any, column schemax.ColumnMeta) (reflect.Value, bool, error) {
	generatedValue := reflect.ValueOf(generated)
	if !generatedValue.IsValid() {
		return reflect.Value{}, false, fmt.Errorf("dbx: generated id is invalid for column %s", column.Name)
	}
	if generatedValue.Type().AssignableTo(targetField.Type()) {
		targetField.Set(generatedValue)
		return targetField, true, nil
	}
	if generatedValue.Type().ConvertibleTo(targetField.Type()) {
		targetField.Set(generatedValue.Convert(targetField.Type()))
		return targetField, true, nil
	}
	return reflect.Value{}, false, nil
}

func (m Mapper[E]) entityValue(entity *E) (reflect.Value, error) {
	if entity == nil {
		return reflect.Value{}, ErrNilEntity
	}
	return reflect.ValueOf(entity).Elem(), nil
}

func (m Mapper[E]) buildAssignment(ctx context.Context, root reflect.Value, column schemax.ColumnMeta, field MappedField, generator idgen.Generator) (querydsl.Assignment, bool, error) {
	if column.PrimaryKey && shouldGenerateID(column) {
		return m.generatedOrExistingAssignment(ctx, root, column, field, generator)
	}
	return buildFieldAssignment(root, column, field)
}

func (m Mapper[E]) generatedOrExistingAssignment(ctx context.Context, root reflect.Value, column schemax.ColumnMeta, field MappedField, generator idgen.Generator) (querydsl.Assignment, bool, error) {
	fieldValue, generated, err := m.ensureGeneratedID(ctx, root, field, column, generator)
	if err != nil {
		return nil, false, err
	}
	if generated {
		return assignmentFromValue(column, field, fieldValue)
	}
	return buildFieldAssignment(root, column, field)
}

func buildFieldAssignment(root reflect.Value, column schemax.ColumnMeta, field MappedField) (querydsl.Assignment, bool, error) {
	fieldValue, err := fieldValueForRead(root, field)
	if err != nil {
		return nil, false, err
	}
	return assignmentFromValue(column, field, fieldValue)
}

func assignmentFromValue(column schemax.ColumnMeta, field MappedField, fieldValue reflect.Value) (querydsl.Assignment, bool, error) {
	boundValue, err := boundFieldValue(field, fieldValue)
	if err != nil {
		return nil, false, err
	}
	return metadataAssignment{meta: column, value: boundValue}, true, nil
}
