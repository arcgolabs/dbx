package validate

import "strings"

func detectStatementType(sql string) string {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "UNKNOWN"
	}
	parts := strings.Fields(sql)
	if len(parts) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(parts[0])
}
