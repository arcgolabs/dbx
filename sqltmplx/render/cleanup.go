package render

import (
	"regexp"
	"strings"

	"github.com/samber/lo"
)

var leadingBool = regexp.MustCompile(`(?is)^\s*(AND|OR)\b`)
var trailingComma = regexp.MustCompile(`(?s),\s*$`)

func cleanupWhere(s string) string {
	s = strings.TrimSpace(s)
	s = leadingBool.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if lo.IsEmpty(s) {
		return ""
	}
	return "WHERE " + s
}

func cleanupSet(s string) string {
	s = strings.TrimSpace(s)
	s = trailingComma.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if lo.IsEmpty(s) {
		return ""
	}
	return "SET " + s
}

func compactWhitespace(s string) string {
	return strings.Join(lo.Compact(strings.Fields(s)), " ")
}
