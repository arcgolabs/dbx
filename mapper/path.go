package mapper

import (
	"fmt"
	"reflect"

	"github.com/DaiYuANg/arcgo/collectionx"
)

func appendIndexPath(prefix []int, index int) []int {
	path := make([]int, len(prefix)+1)
	copy(path, prefix)
	path[len(prefix)] = index
	return path
}

func indirectStructType(typ reflect.Type) (reflect.Type, bool) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, false
	}
	return typ, true
}

func fieldPath(field MappedField) collectionx.List[int] {
	if field.Path.Len() > 0 {
		return field.Path
	}
	return collectionx.NewList[int](field.Index)
}

func ensureFieldValue(root reflect.Value, field MappedField) (reflect.Value, error) {
	return walkFieldValue(root, field, true)
}

func fieldValueForRead(root reflect.Value, field MappedField) (reflect.Value, error) {
	return walkFieldValue(root, field, false)
}

func walkFieldValue(root reflect.Value, field MappedField, createPointers bool) (reflect.Value, error) {
	current := root
	path := fieldPath(field)
	if path.Len() == 0 {
		return reflect.Value{}, fmt.Errorf("dbx: field path for %s is empty", field.Name)
	}
	for i := range path.Len() {
		index, _ := path.Get(i)
		current = current.Field(index)
		if i == path.Len()-1 {
			return current, nil
		}
		next, ok, err := descendFieldValue(current, field, path, i+1, createPointers)
		if err != nil {
			return reflect.Value{}, err
		}
		if !ok {
			return reflect.Zero(field.Type), nil
		}
		current = next
	}
	return reflect.Value{}, fmt.Errorf("dbx: field path for %s is empty", field.Name)
}

func descendFieldValue(current reflect.Value, field MappedField, path collectionx.List[int], depth int, createPointers bool) (reflect.Value, bool, error) {
	descended, ok, err := descendPointerValue(current, field, createPointers)
	if err != nil || !ok {
		return reflect.Value{}, ok, err
	}
	if descended.Kind() != reflect.Struct {
		return reflect.Value{}, false, fmt.Errorf("dbx: field path %v does not resolve to struct", path.Take(depth).Values())
	}
	return descended, true, nil
}

func descendPointerValue(current reflect.Value, field MappedField, createPointers bool) (reflect.Value, bool, error) {
	for current.Kind() == reflect.Pointer {
		next, ok, err := pointerFieldValue(current, field, createPointers)
		if err != nil || !ok {
			return reflect.Value{}, ok, err
		}
		current = next
	}
	return current, true, nil
}

func pointerFieldValue(current reflect.Value, field MappedField, createPointers bool) (reflect.Value, bool, error) {
	if !current.IsNil() {
		return current.Elem(), true, nil
	}
	if createPointers {
		current.Set(reflect.New(current.Type().Elem()))
		return current.Elem(), true, nil
	}
	return missingPointerFieldValue(field)
}

func missingPointerFieldValue(field MappedField) (reflect.Value, bool, error) {
	if field.Type == nil {
		return reflect.Value{}, false, fmt.Errorf("dbx: field %s type metadata is missing", field.Name)
	}
	return reflect.Value{}, false, nil
}

func normalizeFieldValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil
	}
	return value.Interface()
}

func boundFieldValue(field MappedField, value reflect.Value) (any, error) {
	if field.codec == nil {
		return normalizeFieldValue(value), nil
	}
	encoded, err := field.codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("dbx: encode field %s: %w", field.Name, err)
	}
	return encoded, nil
}
