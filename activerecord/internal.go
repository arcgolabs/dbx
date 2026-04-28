package activerecord

import (
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"maps"
	"reflect"
	"slices"

	"github.com/arcgolabs/collectionx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/repository"
)

func (s *Store[E, S]) keyOf(entity *E) repository.Key {
	if entity == nil {
		return nil
	}
	columns := primaryKeyColumns(s.repository.Schema())
	if columns.Len() == 0 {
		return nil
	}
	root := reflect.ValueOf(entity)
	if root.Kind() != reflect.Ptr || root.IsNil() {
		return nil
	}
	root = root.Elem()
	if root.Kind() != reflect.Struct {
		return nil
	}
	key := make(repository.Key, columns.Len())
	var resolveErr error
	columns.Range(func(_ int, column string) bool {
		field, ok := s.repository.Mapper().FieldByColumn(column)
		if !ok {
			resolveErr = fmt.Errorf("dbx: mapped field for column %s not found", column)
			return false
		}
		value, err := mappedFieldValue(root, field)
		if err != nil {
			resolveErr = err
			return false
		}
		key[column] = value.Interface()
		return true
	})
	if resolveErr != nil {
		return nil
	}
	return key
}

func primaryKeyColumns[S querydsl.TableSource](schema S) collectionx.List[string] {
	type primaryKeyProvider interface {
		PrimaryKey() (schemax.PrimaryKeyMeta, bool)
	}
	if provider, ok := any(schema).(primaryKeyProvider); ok {
		if primary, ok := provider.PrimaryKey(); ok && primary.Columns.Len() > 0 {
			return primary.Columns.Clone()
		}
	}
	type primaryColumnProvider interface {
		PrimaryColumn() (schemax.ColumnMeta, bool)
	}
	if provider, ok := any(schema).(primaryColumnProvider); ok {
		if column, ok := provider.PrimaryColumn(); ok && column.Name != "" {
			return collectionx.NewList[string](column.Name)
		}
	}
	return collectionx.NewList[string]("id")
}

func mappedFieldValue(root reflect.Value, field mapperx.MappedField) (reflect.Value, error) {
	value := root
	if field.Path.Len() == 0 {
		return dereferenceMappedValue(value.Field(field.Index)), nil
	}
	var pathErr error
	field.Path.Range(func(_ int, index int) bool {
		next, err := mappedStructField(value, field.Name, index)
		if err != nil {
			pathErr = err
			return false
		}
		value = next
		return true
	})
	if pathErr != nil {
		return reflect.Value{}, pathErr
	}
	return dereferenceMappedValue(value), nil
}

func mappedStructField(value reflect.Value, fieldName string, index int) (reflect.Value, error) {
	structValue, err := requireStructValue(value, fieldName)
	if err != nil {
		return reflect.Value{}, err
	}
	return structValue.Field(index), nil
}

func requireStructValue(value reflect.Value, fieldName string) (reflect.Value, error) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}, fmt.Errorf("dbx: nil pointer for field %s", fieldName)
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("dbx: field %s path reaches non-struct", fieldName)
	}
	return value, nil
}

func dereferenceMappedValue(value reflect.Value) reflect.Value {
	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Zero(value.Type().Elem())
		}
		value = value.Elem()
	}
	return value
}

func cloneKey(key repository.Key) repository.Key {
	if len(key) == 0 {
		return nil
	}
	return maps.Clone(key)
}

func hasZeroKeyValue(key repository.Key) bool {
	return slices.ContainsFunc(slices.Collect(maps.Values(key)), isZeroKeyValue)
}

func isZeroKeyValue(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return true
	}

	if isNilKeyValue(rv) {
		return true
	}

	return rv.IsZero()
}

func isNilKeyValue(value reflect.Value) bool {
	kind := value.Kind()
	return (kind == reflect.Ptr || kind == reflect.Interface) && value.IsNil()
}
