package render

import (
	"regexp"
	"strings"
)

var leadingBool = regexp.MustCompile(`(?is)^\s*(AND|OR)\b`)
var trailingComma = regexp.MustCompile(`(?s),\s*$`)

func cleanupWhere(s string) string {
	s = strings.TrimSpace(s)
	s = leadingBool.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return "WHERE " + s
}

func cleanupSet(s string) string {
	s = strings.TrimSpace(s)
	s = trailingComma.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return "SET " + s
}

func compactWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
