package schema

import (
	"reflect"
	"strings"
	"unicode"

	"github.com/samber/lo"
)

func resolveColumnName(field reflect.StructField) string {
	keys := []string{"column", "dbx", "json"}
	key, ok := lo.Find(keys, func(key string) bool {
		raw := strings.TrimSpace(field.Tag.Get(key))
		if raw == "" || raw == "-" {
			return false
		}
		name := strings.TrimSpace(strings.Split(raw, ",")[0])
		return name != "" && name != "-"
	})
	if !ok {
		return toSnakeCase(field.Name)
	}
	raw := strings.TrimSpace(field.Tag.Get(key))
	return strings.TrimSpace(strings.Split(raw, ",")[0])
}

func resolveTagNameAndOptions(field reflect.StructField) (string, map[string]string) {
	raw := strings.TrimSpace(field.Tag.Get("dbx"))
	if raw != "" && raw != "-" {
		parts := strings.Split(raw, ",")
		name := strings.TrimSpace(parts[0])
		if name == "" {
			name = toSnakeCase(field.Name)
		}
		return name, associateTagOptions(parts[1:])
	}

	return resolveColumnName(field), map[string]string{}
}

func parseTagOptions(raw string) map[string]string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "-" {
		return map[string]string{}
	}
	parts := strings.Split(trimmed, ",")
	return associateTagOptions(parts)
}

func associateTagOptions(parts []string) map[string]string {
	pairs := lo.FilterMap(parts, func(part string, _ int) (lo.Entry[string, string], bool) {
		k, v := splitTagOption(part)
		if k == "" {
			return lo.Entry[string, string]{}, false
		}
		return lo.Entry[string, string]{Key: k, Value: v}, true
	})
	return lo.Associate(pairs, func(e lo.Entry[string, string]) (string, string) { return e.Key, e.Value })
}

func splitTagOption(raw string) (string, string) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return "", ""
	}
	if key, value, ok := strings.Cut(trimmed, "="); ok {
		return strings.TrimSpace(key), strings.TrimSpace(value)
	}
	return trimmed, "true"
}

func optionEnabled(options map[string]string, key string) bool {
	value, ok := options[strings.ToLower(key)]
	if !ok {
		return false
	}
	trimmed := strings.TrimSpace(value)
	return trimmed == "" || trimmed == "true"
}

func optionValue(options map[string]string, key string) string {
	return strings.TrimSpace(options[strings.ToLower(key)])
}

func splitReference(input string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	table := strings.TrimSpace(parts[0])
	column := strings.TrimSpace(parts[1])
	if table == "" || column == "" {
		return "", "", false
	}
	return table, column, true
}

func parseReferentialAction(input string) ReferentialAction {
	switch strings.ToUpper(strings.TrimSpace(input)) {
	case string(ReferentialCascade):
		return ReferentialCascade
	case string(ReferentialSetNull):
		return ReferentialSetNull
	case string(ReferentialSetDefault):
		return ReferentialSetDefault
	case string(ReferentialRestrict):
		return ReferentialRestrict
	case string(ReferentialNoAction):
		return ReferentialNoAction
	default:
		return ""
	}
}

func toSnakeCase(input string) string {
	if input == "" {
		return ""
	}

	out := make([]rune, 0, len(input)+4)

	for index, r := range input {
		if unicode.IsUpper(r) {
			if shouldInsertSnakeCaseUnderscore(input, index) {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
			continue
		}
		out = append(out, r)
	}

	return string(out)
}

func shouldInsertSnakeCaseUnderscore(input string, index int) bool {
	if index == 0 {
		return false
	}
	prev := rune(input[index-1])
	if prev == '_' {
		return false
	}
	if !unicode.IsUpper(prev) {
		return true
	}
	return index+1 < len(input) && unicode.IsLower(rune(input[index+1]))
}
