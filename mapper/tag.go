package mapper

import (
	"strings"
	"unicode"
)

func associateTagOptions(parts []string) map[string]string {
	options := make(map[string]string, len(parts))
	for _, part := range parts {
		key, value := splitTagOption(part)
		if key != "" {
			options[key] = value
		}
	}
	return options
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
