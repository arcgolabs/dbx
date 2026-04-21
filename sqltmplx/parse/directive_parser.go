package parse

import (
	"fmt"
	"regexp"
	"strings"
)

var nilRegex = regexp.MustCompile(`\bnil\b`)

func parseDirective(input string) (*Directive, error) {
	raw := strings.TrimSpace(input)
	if !strings.HasPrefix(raw, "%") {
		return nil, fmt.Errorf("sqltmplx: directive %q must start with %%", raw)
	}
	normalized := strings.TrimSpace(strings.TrimPrefix(raw, "%"))
	switch {
	case normalized == "where":
		return &Directive{Where: &WhereDirective{Keyword: "where"}}, nil
	case normalized == "set":
		return &Directive{Set: &SetDirective{Keyword: "set"}}, nil
	case normalized == "end":
		return &Directive{End: &EndDirective{Keyword: "end"}}, nil
	case strings.HasPrefix(normalized, "if"):
		exprText := strings.TrimSpace(strings.TrimPrefix(normalized, "if"))
		if exprText == "" {
			return nil, fmt.Errorf("sqltmplx: parse directive %q: missing if expression", normalized)
		}
		return &Directive{
			If: &IfDirective{
				Keyword: "if",
				Expr:    normalizeExpr(exprText),
			},
		}, nil
	default:
		return nil, fmt.Errorf("sqltmplx: parse directive %q: unsupported directive", normalized)
	}
}

func normalizeExpr(in string) string {
	in = strings.TrimSpace(in)
	return nilRegex.ReplaceAllString(in, "nil")
}
