package dbx_test

import (
	"fmt"
	"strings"
)

type testSQLiteDialect struct{}

func (testSQLiteDialect) Name() string         { return "sqlite" }
func (testSQLiteDialect) BindVar(_ int) string { return "?" }
func (testSQLiteDialect) QuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (testSQLiteDialect) RenderLimitOffset(limit, offset *int) (string, error) {
	if limit == nil && offset == nil {
		return "", nil
	}
	if limit != nil && offset != nil {
		return fmt.Sprintf("LIMIT %d OFFSET %d", *limit, *offset), nil
	}
	if limit != nil {
		return fmt.Sprintf("LIMIT %d", *limit), nil
	}
	return fmt.Sprintf("LIMIT -1 OFFSET %d", *offset), nil
}
