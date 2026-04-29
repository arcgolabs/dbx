package querydsl

import (
	"errors"
	"reflect"
	"strings"
	"unicode"

	schemax "github.com/arcgolabs/dbx/schema"
)

type sourceColumnBinder interface {
	ValueType() reflect.Type
	bindSourceColumn(schemax.ColumnMeta) any
}

// Source binds a struct-shaped query source for CTEs, views, and derived tables.
//
// The shape should embed querydsl.Table and expose querydsl.Column[T] fields.
// Column names are read from `dbx` tags or inferred from field names.
func Source[S any](name string, shape S) (S, error) {
	return SourceAs(name, "", shape)
}

// MustSource is Source and panics on invalid source declarations.
func MustSource[S any](name string, shape S) S {
	bound, err := Source(name, shape)
	if err != nil {
		panic(err)
	}
	return bound
}

// SourceAs binds a struct-shaped query source with an alias.
func SourceAs[S any](name, alias string, shape S) (S, error) {
	if strings.TrimSpace(name) == "" {
		return shape, errors.New("dbx/querydsl: source name cannot be empty")
	}
	table := NewTableRef(name, alias, nil, nil)
	value, err := sourceShapeValue(&shape)
	if err != nil {
		return shape, err
	}
	bindSourceFields(value, table)
	return shape, nil
}

// MustSourceAs is SourceAs and panics on invalid source declarations.
func MustSourceAs[S any](name, alias string, shape S) S {
	bound, err := SourceAs(name, alias, shape)
	if err != nil {
		panic(err)
	}
	return bound
}

func sourceShapeValue(shape any) (reflect.Value, error) {
	value := reflect.ValueOf(shape)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}, errors.New("dbx/querydsl: source shape is nil")
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("dbx/querydsl: source shape must be a struct")
	}
	return value, nil
}

func bindSourceFields(value reflect.Value, table Table) {
	tableType := reflect.TypeFor[Table]()
	valueType := value.Type()
	for i := range value.NumField() {
		fieldValue := value.Field(i)
		fieldType := valueType.Field(i)
		if !fieldValue.CanSet() {
			continue
		}
		if fieldValue.Type() == tableType {
			fieldValue.Set(reflect.ValueOf(table))
			continue
		}
		binder, ok := fieldValue.Interface().(sourceColumnBinder)
		if !ok {
			continue
		}
		name, ok := sourceColumnName(fieldType)
		if !ok {
			continue
		}
		meta := schemax.ColumnMeta{
			Name:      name,
			Table:     table.Name(),
			Alias:     table.Alias(),
			FieldName: fieldType.Name,
			GoType:    binder.ValueType(),
		}
		fieldValue.Set(reflect.ValueOf(binder.bindSourceColumn(meta)))
	}
}

func sourceColumnName(field reflect.StructField) (string, bool) {
	tag := strings.TrimSpace(field.Tag.Get("dbx"))
	if tag == "-" {
		return "", false
	}
	if tag != "" {
		name, _, _ := strings.Cut(tag, ",")
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			return trimmed, true
		}
	}
	return toSourceColumnName(field.Name), true
}

func toSourceColumnName(input string) string {
	out := make([]rune, 0, len(input)+4)
	for index, r := range input {
		if unicode.IsUpper(r) {
			if index > 0 {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
			continue
		}
		out = append(out, r)
	}
	return string(out)
}
