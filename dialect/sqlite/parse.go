package sqlite

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"regexp"
	"strings"
)

func parseCreateTableChecks(createSQL string) []schemax.CheckState {
	upper := strings.ToUpper(createSQL)
	checks := make([]schemax.CheckState, 0, 2)

	for offset := 0; ; {
		expression, nextOffset, found := nextSQLiteCheckExpression(createSQL, upper, offset)
		if !found {
			return checks
		}
		if expression != "" {
			checks = append(checks, schemax.CheckState{Expression: expression})
		}
		offset = nextOffset
	}
}

func nextSQLiteCheckExpression(createSQL, upper string, offset int) (string, int, bool) {
	index := strings.Index(upper[offset:], "CHECK")
	if index < 0 {
		return "", 0, false
	}

	index += offset
	start := strings.Index(createSQL[index:], "(")
	if start < 0 {
		return "", index + len("CHECK"), true
	}
	start += index

	end := sqliteMatchingParen(createSQL, start)
	if end < 0 {
		return "", len(createSQL), false
	}

	return strings.TrimSpace(createSQL[start+1 : end]), end + 1, true
}

func sqliteMatchingParen(input string, start int) int {
	depth := 0
	for i := start; i < len(input); i++ {
		switch input[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseCreateTableAutoincrementColumns(createSQL string) []string {
	matches := sqliteAutoincrementPattern.FindAllStringSubmatch(createSQL, -1)
	columns := make([]string, 0, len(matches))
	for i := range matches {
		match := matches[i]
		if len(match) >= 2 {
			columns = append(columns, strings.TrimSpace(match[1]))
		}
	}
	return columns
}

func referentialAction(value string) schemax.ReferentialAction {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case string(schemax.ReferentialCascade):
		return schemax.ReferentialCascade
	case string(schemax.ReferentialRestrict):
		return schemax.ReferentialRestrict
	case string(schemax.ReferentialSetNull):
		return schemax.ReferentialSetNull
	case string(schemax.ReferentialSetDefault):
		return schemax.ReferentialSetDefault
	case string(schemax.ReferentialNoAction):
		return schemax.ReferentialNoAction
	default:
		return ""
	}
}

var sqliteAutoincrementPattern = regexp.MustCompile(`(?i)"?([a-zA-Z0-9_]+)"?\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT`)
