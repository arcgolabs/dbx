package parse

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqltmpl/scan"
)

var (
	errUnterminatedSQLComment = errors.New("sqltmpl: unterminated sql comment")
	errInvalidCastSuffix      = errors.New("invalid cast suffix")
	errUnterminatedBalanced   = errors.New("unterminated balanced literal")
	errUnterminatedQuoted     = errors.New("unterminated quoted literal")
	errEmptyScalarLiteral     = errors.New("empty scalar literal")
)

func compileTextToken(tok scan.Token) (*collectionx.List[Node], error) {
	nodes := collectionx.NewList[Node]()
	input := tok.Value
	textStart := 0
	for i := 0; i < len(input); {
		if i+1 < len(input) && input[i] == '/' && input[i+1] == '*' {
			param, next, handled, err := compileCommentPlaceholder(input, i, tok.Span.Start)
			if err != nil {
				return nil, err
			}
			if handled {
				appendTextFragment(nodes, input, textStart, i, tok.Span.Start)
				nodes.Add(param)
				i = next
				textStart = next
				continue
			}
		}
		i++
	}
	appendTextFragment(nodes, input, textStart, len(input), tok.Span.Start)
	return nodes, nil
}

func appendTextFragment(nodes *collectionx.List[Node], input string, start, end int, base scan.Position) {
	if start >= end {
		return
	}
	text := input[start:end]
	startPos := scan.AdvancePosition(base, input[:start])
	endPos := scan.AdvancePosition(startPos, text)
	nodes.Add(TextNode{Text: text, Span: scan.Span{Start: startPos, End: endPos}})
}

func compileCommentPlaceholder(input string, start int, base scan.Position) (Node, int, bool, error) {
	raw, commentEnd, handled, err := parseCommentPlaceholder(input, start)
	if err != nil || !handled {
		return nil, commentEnd, handled, wrapParseError(scan.AdvancePosition(base, input[:start]), err)
	}

	sampleStart, err := placeholderSampleStart(input, commentEnd, raw)
	if err != nil {
		return nil, 0, false, wrapParseError(scan.AdvancePosition(base, input[:start]), err)
	}

	end, err := skipPlaceholderSample(input, sampleStart)
	if err != nil {
		return nil, 0, false, wrapParseError(scan.AdvancePosition(base, input[:start]), fmt.Errorf("sqltmpl: placeholder %q invalid test literal: %w", raw, err))
	}

	spread := input[sampleStart] == '(' || looksLikeCollectionSample(input, sampleStart)
	startPos := scan.AdvancePosition(base, input[:start])
	endPos := scan.AdvancePosition(base, input[:end])
	return ParamNode{Name: raw, Spread: spread, Span: scan.Span{Start: startPos, End: endPos}}, end, true, nil
}

func parseCommentPlaceholder(input string, start int) (string, int, bool, error) {
	endComment := strings.Index(input[start+2:], "*/")
	if endComment < 0 {
		return "", 0, false, errUnterminatedSQLComment
	}

	commentEnd := start + 2 + endComment + 2
	raw := strings.TrimSpace(input[start+2 : start+2+endComment])
	if raw == "" || strings.HasPrefix(raw, "%") || !isParamPath(raw) {
		return "", commentEnd, false, nil
	}
	return raw, commentEnd, true, nil
}

func placeholderSampleStart(input string, commentEnd int, raw string) (int, error) {
	start := skipSpaces(input, commentEnd)
	if start >= len(input) {
		return 0, fmt.Errorf("sqltmpl: placeholder %q missing test literal", raw)
	}
	return start, nil
}

func isParamPath(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '.':
			if i == 0 || i == len(s)-1 {
				return false
			}
		case r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r):
		default:
			return false
		}
	}
	return true
}

func looksLikeCollectionSample(input string, start int) bool {
	i := start
	if i < len(input) && isIdentifierStart(rune(input[i])) {
		j := i + 1
		for j < len(input) && isIdentifierPart(rune(input[j])) {
			j++
		}
		return j < len(input) && input[j] == '['
	}
	return false
}

func skipPlaceholderSample(input string, start int) (int, error) {
	i := start
	switch {
	case input[i] == '(':
		var err error
		i, err = skipBalanced(input, i, '(', ')')
		if err != nil {
			return 0, err
		}
	case input[i] == '\'' || input[i] == '"':
		var err error
		i, err = skipQuoted(input, i)
		if err != nil {
			return 0, err
		}
	case isIdentifierStart(rune(input[i])):
		var err error
		i, err = skipIdentifierExpr(input, i)
		if err != nil {
			return 0, err
		}
	default:
		var err error
		i, err = skipScalarToken(input, i)
		if err != nil {
			return 0, err
		}
	}
	return skipExprSuffixes(input, i)
}
