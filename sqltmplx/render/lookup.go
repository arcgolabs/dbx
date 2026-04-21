package render

import (
	"reflect"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/samber/lo"
)

func lookupValue(params any, name string) (any, bool) {
	cur := params
	remaining := name
	for remaining != "" {
		part, rest, found := strings.Cut(remaining, ".")
		remaining = rest

		next, ok := lookupOneValue(cur, part)
		if !ok {
			return nil, false
		}
		cur = next
		if !found {
			break
		}
	}
	return cur, true
}

func lookupOneValue(params any, name string) (any, bool) {
	if provider, ok := params.(paramLookup); ok {
		value, exists := provider.LookupSQLTemplateParam(name)
		if exists {
			return value, true
		}
	}
	v, ok := indirectValue(params)
	if !ok {
		return nil, false
	}
	if v.Kind() == reflect.Map {
		return lookupMapValue(v, name)
	}
	if v.Kind() == reflect.Struct {
		return lookupStructValue(v, name)
	}
	return nil, false
}

func lookupStructValue(v reflect.Value, name string) (any, bool) {
	meta := cachedStructMetadata(v.Type())
	if field, exists := lookupStructField(meta, name); exists {
		return v.Field(field.index).Interface(), true
	}
	if value, ok := callZeroArgMethod(v, name); ok {
		return value, true
	}
	return nil, false
}

func lookupStructField(meta *structMetadata, name string) (structFieldMetadata, bool) {
	if field, exists := meta.lookup.Get(name); exists {
		return field, true
	}
	folded := strings.ToLower(name)
	if folded == name {
		return structFieldMetadata{}, false
	}
	return meta.lookup.Get(folded)
}

func callZeroArgMethod(v reflect.Value, name string) (any, bool) {
	if value, ok := callZeroArgMethodOn(v, name); ok {
		return value, true
	}
	if v.Kind() != reflect.Pointer && v.CanAddr() {
		return callZeroArgMethodOn(v.Addr(), name)
	}
	return nil, false
}

func callZeroArgMethodOn(v reflect.Value, name string) (any, bool) {
	index, ok := lookupZeroArgMethodIndex(cachedMethodMetadata(v.Type()), name)
	if !ok {
		return nil, false
	}
	return v.Method(index).Call(nil)[0].Interface(), true
}

func lookupZeroArgMethodIndex(meta *methodMetadata, name string) (int, bool) {
	if index, exists := meta.lookup.Get(name); exists {
		return index, true
	}
	folded := strings.ToLower(name)
	if folded == name {
		return 0, false
	}
	return meta.lookup.Get(folded)
}

func lookupMapValue(v reflect.Value, name string) (any, bool) {
	if v.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	if value, ok := reflectMapStringValue(v, name); ok {
		return value, true
	}
	if folded := strings.ToLower(name); folded != name {
		if value, ok := reflectMapStringValue(v, folded); ok {
			return value, true
		}
	}
	if upper := strings.ToUpper(name); upper != name {
		if value, ok := reflectMapStringValue(v, upper); ok {
			return value, true
		}
	}
	return nil, false
}

func reflectMapStringValue(v reflect.Value, key string) (any, bool) {
	mapKey := reflect.ValueOf(key)
	if keyType := v.Type().Key(); mapKey.Type() != keyType && mapKey.Type().ConvertibleTo(keyType) {
		mapKey = mapKey.Convert(keyType)
	}
	mv := v.MapIndex(mapKey)
	if !mv.IsValid() {
		return nil, false
	}
	return mv.Interface(), true
}

func fieldAliases(f reflect.StructField) collectionx.List[string] {
	aliases := collectionx.NewListWithCapacity[string](3)
	seen := collectionx.NewSetWithCapacity[string](3)
	for _, tagKey := range [...]string{"sqltmpl", "db", "json"} {
		raw := strings.TrimSpace(f.Tag.Get(tagKey))
		if raw == "" || raw == "-" {
			continue
		}
		alias := strings.TrimSpace(strings.Split(raw, ",")[0])
		if alias == "" || alias == "-" {
			continue
		}
		if seen.Contains(alias) {
			continue
		}
		seen.Add(alias)
		aliases.Add(alias)
	}
	return aliases
}

func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	rv, ok := indirectValue(v)
	if !ok {
		return true
	}
	if isLengthValue(rv.Kind()) {
		return rv.Len() == 0
	}
	return false
}

func isLengthValue(kind reflect.Kind) bool {
	return kind == reflect.String || kind == reflect.Array || kind == reflect.Slice || kind == reflect.Map
}

func isBlank(v any) bool {
	if v == nil {
		return true
	}
	rv, ok := indirectValue(v)
	if !ok {
		return true
	}
	return lo.IsEmpty(rv.Interface())
}

func isPresent(v any) bool {
	return !isBlank(v)
}
