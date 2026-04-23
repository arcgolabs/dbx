package mapper

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/arcgolabs/collectionx"
	codecx "github.com/arcgolabs/dbx/codec"
	"github.com/samber/hot"
)

type mapperMetadata struct {
	entityType         reflect.Type
	fields             collectionx.List[MappedField]
	byColumn           collectionx.Map[string, MappedField]
	byNormalizedColumn collectionx.Map[string, MappedField]
	scanPlans          *hot.HotCache[string, *scanPlan]
}

func buildMapperMetadata(entityType reflect.Type, codecs *codecx.Registry) (*mapperMetadata, error) {
	if entityType.Kind() != reflect.Struct {
		return nil, ErrUnsupportedEntity
	}

	fields := collectionx.NewListWithCapacity[MappedField](entityType.NumField())
	byColumn := collectionx.NewMapWithCapacity[string, MappedField](entityType.NumField())
	byNormalizedColumn := collectionx.NewMapWithCapacity[string, MappedField](entityType.NumField())
	if err := collectMappedFields(entityType, nil, fields, byColumn, byNormalizedColumn, codecs); err != nil {
		return nil, err
	}

	return &mapperMetadata{
		entityType:         entityType,
		fields:             fields,
		byColumn:           byColumn,
		byNormalizedColumn: byNormalizedColumn,
		scanPlans:          hot.NewHotCache[string, *scanPlan](hot.LRU, 128).Build(),
	}, nil
}

func resolveEntityColumn(field reflect.StructField) (string, map[string]string) {
	raw := strings.TrimSpace(field.Tag.Get("dbx"))
	if raw == "-" {
		return "", nil
	}
	if raw == "" {
		return toSnakeCase(field.Name), map[string]string{}
	}

	parts := strings.Split(raw, ",")
	name := strings.TrimSpace(parts[0])
	if name == "" {
		name = toSnakeCase(field.Name)
	}
	return name, associateTagOptions(parts[1:])
}

func collectMappedFields(entityType reflect.Type, prefix []int, fields collectionx.List[MappedField], byColumn, byNormalizedColumn collectionx.Map[string, MappedField], codecs *codecx.Registry) error {
	for fieldIndex := range entityType.NumField() {
		field := entityType.Field(fieldIndex)
		path := appendIndexPath(prefix, fieldIndex)
		if err := processField(field, path, fields, byColumn, byNormalizedColumn, codecs); err != nil {
			return err
		}
	}
	return nil
}

func processField(field reflect.StructField, path []int, fields collectionx.List[MappedField], byColumn, byNormalizedColumn collectionx.Map[string, MappedField], codecs *codecx.Registry) error {
	if !field.IsExported() {
		return nil
	}
	rawTag := strings.TrimSpace(field.Tag.Get("dbx"))
	if rawTag == "-" {
		return nil
	}

	// anonymous embedded struct without explicit tag
	if field.Anonymous && rawTag == "" {
		if embeddedType, ok := indirectStructType(field.Type); ok {
			return collectMappedFields(embeddedType, path, fields, byColumn, byNormalizedColumn, codecs)
		}
		return nil
	}

	columnName, options := resolveEntityColumn(field)
	if optionEnabled(options, "inline") {
		embeddedType, ok := indirectStructType(field.Type)
		if !ok {
			return fmt.Errorf("dbx: inline field %s must be a struct or pointer to struct", field.Name)
		}
		return collectMappedFields(embeddedType, path, fields, byColumn, byNormalizedColumn, codecs)
	}
	if columnName == "" {
		return nil
	}

	return addMappedField(field, columnName, options, path, fields, byColumn, byNormalizedColumn, codecs)
}

func addMappedField(field reflect.StructField, columnName string, options map[string]string, path []int, fields collectionx.List[MappedField], byColumn, byNormalizedColumn collectionx.Map[string, MappedField], codecs *codecx.Registry) error {
	codecName := codecx.NormalizeName(optionValue(options, "codec"))
	codec, err := resolveMappedFieldCodec(codecs, codecName)
	if err != nil {
		return fmt.Errorf("dbx: field %s: %w", field.Name, err)
	}

	mapped := MappedField{
		Name:       field.Name,
		Column:     columnName,
		Codec:      codecName,
		Index:      path[0],
		Path:       collectionx.NewList[int](path...),
		Type:       field.Type,
		Insertable: !optionEnabled(options, "readonly") && !optionEnabled(options, "-insert") && !optionEnabled(options, "noinsert"),
		Updatable:  !optionEnabled(options, "readonly") && !optionEnabled(options, "-update") && !optionEnabled(options, "noupdate"),
		codec:      codec,
	}
	fields.Add(mapped)
	byColumn.Set(columnName, mapped)
	normalized := normalizeResultColumnName(columnName)
	if normalized != "" {
		byNormalizedColumn.Set(normalized, mapped)
	}
	return nil
}

func resolveMappedFieldCodec(codecs *codecx.Registry, name string) (codecx.Codec, error) {
	if name == "" {
		var codec codecx.Codec
		return codec, nil
	}
	codec, ok := codecs.Lookup(name)
	if !ok {
		return nil, &codecx.UnknownError{Name: name}
	}
	return codec, nil
}
