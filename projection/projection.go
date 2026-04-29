// Package projection builds select projections from dbx mappers and schema resources.
package projection

import (
	"fmt"

	listx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type Mapper interface {
	Fields() *listx.List[mapperx.MappedField]
}

type columnSelectItem struct {
	meta schemax.ColumnMeta
}

func Of(schema schemax.Resource, mapper Mapper) (*listx.List[querydsl.SelectItem], error) {
	return ofSpec(schema.Spec(), mapper)
}

func Must(schema schemax.Resource, mapper Mapper) *listx.List[querydsl.SelectItem] {
	items, err := Of(schema, mapper)
	if err != nil {
		panic(err)
	}
	return items
}

func Select(schema schemax.Resource, mapper Mapper) (*querydsl.SelectQuery, error) {
	items, err := Of(schema, mapper)
	if err != nil {
		return nil, err
	}
	return querydsl.SelectList(items).From(schema), nil
}

func MustSelect(schema schemax.Resource, mapper Mapper) *querydsl.SelectQuery {
	query, err := Select(schema, mapper)
	if err != nil {
		panic(err)
	}
	return query
}

func ofSpec(spec schemax.TableSpec, mapper Mapper) (*listx.List[querydsl.SelectItem], error) {
	fields := mapper.Fields()
	columnsByName := mappingx.AssociateList[schemax.ColumnMeta, string, schemax.ColumnMeta](spec.Columns, func(_ int, column schemax.ColumnMeta) (string, schemax.ColumnMeta) {
		return column.Name, column
	})

	if unmapped, ok := listx.FindList[mapperx.MappedField](fields, func(_ int, field mapperx.MappedField) bool {
		_, ok := columnsByName.Get(field.Column)
		return !ok
	}); ok {
		return nil, &mapperx.UnmappedColumnError{Column: unmapped.Column}
	}

	return listx.FilterMapList[mapperx.MappedField, querydsl.SelectItem](fields, func(_ int, field mapperx.MappedField) (querydsl.SelectItem, bool) {
		column, ok := columnsByName.Get(field.Column)
		if !ok {
			return nil, false
		}
		return columnSelectItem{meta: column}, true
	}), nil
}

func (s columnSelectItem) QueryExpression() {}
func (s columnSelectItem) QuerySelectItem() {}
func (s columnSelectItem) ColumnRef() schemax.ColumnMeta {
	return s.meta
}

func (s columnSelectItem) RenderOperand(state *querydsl.State) (string, error) {
	var builder querydsl.Buffer
	table := s.meta.Table
	if s.meta.Alias != "" {
		table = s.meta.Alias
	}
	builder.WriteString(state.Dialect().QuoteIdent(table))
	builder.WriteRawByte('.')
	builder.WriteString(state.Dialect().QuoteIdent(s.meta.Name))
	return builder.String(), builder.Err("render projection column operand")
}

func (s columnSelectItem) RenderSelectItem(state *querydsl.State) error {
	operand, err := s.RenderOperand(state)
	if err != nil {
		return fmt.Errorf("dbx/projection: render select item: %w", err)
	}
	state.WriteString(operand)
	return nil
}
