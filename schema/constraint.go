package schema

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/samber/lo"
)

type constraintBinder interface {
	bindConstraint(constraintBinding) any
}

type keyMetadata interface {
	keyMeta() keyBindingMeta
}

type constraintBinding struct {
	indexes    []IndexMeta
	primaryKey *PrimaryKeyMeta
	check      *CheckMeta
}

type keyBindingMeta struct {
	unique  bool
	primary bool
}

type Index[E any] struct {
	meta IndexMeta
}

type Unique[E any] struct {
	meta IndexMeta
}

type CompositeKey[E any] struct {
	meta PrimaryKeyMeta
}

type Check[E any] struct {
	meta CheckMeta
}

func (Index[E]) keyMeta() keyBindingMeta        { return keyBindingMeta{} }
func (Unique[E]) keyMeta() keyBindingMeta       { return keyBindingMeta{unique: true} }
func (CompositeKey[E]) keyMeta() keyBindingMeta { return keyBindingMeta{primary: true} }

func (i Index[E]) bindConstraint(binding constraintBinding) any {
	if len(binding.indexes) > 0 {
		i.meta = cloneIndexMeta(binding.indexes[0])
	}
	return i
}

func (u Unique[E]) bindConstraint(binding constraintBinding) any {
	if len(binding.indexes) > 0 {
		u.meta = cloneIndexMeta(binding.indexes[0])
	}
	return u
}

func (c CompositeKey[E]) bindConstraint(binding constraintBinding) any {
	if binding.primaryKey != nil {
		c.meta = clonePrimaryKeyMeta(*binding.primaryKey)
	}
	return c
}

func (c Check[E]) bindConstraint(binding constraintBinding) any {
	if binding.check != nil {
		c.meta = cloneCheckMeta(*binding.check)
	}
	return c
}

func (i Index[E]) Meta() IndexMeta             { return cloneIndexMeta(i.meta) }
func (u Unique[E]) Meta() IndexMeta            { return cloneIndexMeta(u.meta) }
func (c CompositeKey[E]) Meta() PrimaryKeyMeta { return clonePrimaryKeyMeta(c.meta) }
func (c Check[E]) Meta() CheckMeta             { return cloneCheckMeta(c.meta) }

func resolveConstraintBinding(def schemaTable, field reflect.StructField, value any) (constraintBinding, error) {
	if key, ok := value.(keyMetadata); ok {
		return resolveKeyConstraintBinding(def, field, key)
	}
	return resolveCheckConstraintBinding(def, field)
}

func resolveKeyConstraintBinding(def schemaTable, field reflect.StructField, key keyMetadata) (constraintBinding, error) {
	meta := key.keyMeta()
	options := parseTagOptions(field.Tag.Get(constraintTagName(meta)))
	columns := splitColumnsOption(optionValue(options, "columns"))
	if len(columns) == 0 {
		return constraintBinding{}, fmt.Errorf("dbx: constraint %s on schema %s requires columns option", field.Name, schemaTypeName(def))
	}
	name := optionValue(options, "name")
	if name == "" {
		name = defaultConstraintName(def.Name(), field.Name, meta)
	}
	if meta.primary {
		return constraintBinding{
			primaryKey: &PrimaryKeyMeta{
				Name:    name,
				Table:   def.Name(),
				Columns: collectionx.NewList[string](columns...),
			},
		}, nil
	}
	return constraintBinding{
		indexes: []IndexMeta{{
			Name:    name,
			Table:   def.Name(),
			Columns: collectionx.NewList[string](columns...),
			Unique:  meta.unique,
		}},
	}, nil
}

func constraintTagName(meta keyBindingMeta) string {
	if meta.primary {
		return "key"
	}
	return "idx"
}

func resolveCheckConstraintBinding(def schemaTable, field reflect.StructField) (constraintBinding, error) {
	options := parseTagOptions(field.Tag.Get("check"))
	expression := strings.TrimSpace(optionValue(options, "expr"))
	if expression == "" {
		return constraintBinding{}, fmt.Errorf("dbx: check constraint %s on schema %s requires expr option", field.Name, schemaTypeName(def))
	}
	name := optionValue(options, "name")
	if name == "" {
		name = "ck_" + def.Name() + "_" + toSnakeCase(field.Name)
	}
	return constraintBinding{
		check: &CheckMeta{
			Name:       name,
			Table:      def.Name(),
			Expression: expression,
		},
	}, nil
}

func schemaTypeName(def schemaTable) string {
	if typ := def.SchemaType(); typ != nil {
		return typ.Name()
	}
	return def.Name()
}

func splitColumnsOption(value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == '|' || r == ','
	})
	return lo.Compact(lo.Map(parts, func(part string, _ int) string {
		return strings.TrimSpace(part)
	}))
}

func defaultConstraintName(table, field string, meta keyBindingMeta) string {
	prefix := "idx"
	if meta.unique {
		prefix = "ux"
	}
	if meta.primary {
		prefix = "pk"
	}
	return prefix + "_" + table + "_" + toSnakeCase(field)
}

func cloneIndexMeta(meta IndexMeta) IndexMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func clonePrimaryKeyMeta(meta PrimaryKeyMeta) PrimaryKeyMeta {
	meta.Columns = meta.Columns.Clone()
	return meta
}

func clonePrimaryKeyMetaPtr(meta *PrimaryKeyMeta) *PrimaryKeyMeta {
	if meta == nil {
		return nil
	}
	return new(clonePrimaryKeyMeta(*meta))
}

func cloneForeignKeyMeta(meta ForeignKeyMeta) ForeignKeyMeta {
	meta.Columns = meta.Columns.Clone()
	meta.TargetColumns = meta.TargetColumns.Clone()
	return meta
}

func cloneCheckMeta(meta CheckMeta) CheckMeta {
	return meta
}
