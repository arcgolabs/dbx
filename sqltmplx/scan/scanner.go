package scan

import (
	"errors"
	"strings"

	"github.com/arcgolabs/collectionx"
)

// Scan tokenizes a SQL template string.
func Scan(input string) ([]Token, error) {
	tokens, err := ScanList(input)
	if err != nil {
		return nil, err
	}
	return tokens.Values(), nil
}

// ScanList tokenizes a SQL template string into a collectionx.List.
func ScanList(input string) (collectionx.List[Token], error) {
	var (
		tokens  = collectionx.NewList[Token]()
		textBuf strings.Builder
		textPos = Position{Line: 1, Column: 1}
		pos     = textPos
	)

	flushText := func(end Position) {
		if textBuf.Len() == 0 {
			return
		}
		value := textBuf.String()
		tokens.Add(Token{Kind: Text, Value: value, Span: Span{Start: textPos, End: end}})
		textBuf.Reset()
		textPos = end
	}

	for input != "" {
		start := strings.Index(input, "/*")
		if start < 0 {
			writeBuilderString(&textBuf, input)
			pos = AdvancePosition(pos, input)
			break
		}

		writeBuilderString(&textBuf, input[:start])
		pos = AdvancePosition(pos, input[:start])
		input = input[start+2:]
		commentStart := pos
		pos = AdvancePosition(pos, "/*")

		end := strings.Index(input, "*/")
		if end < 0 {
			return nil, errors.New("sqltmplx: unterminated directive comment")
		}

		rawBody := input[:end]
		raw := strings.TrimSpace(rawBody)
		fullComment := "/*" + rawBody + "*/"
		pos = AdvancePosition(pos, rawBody)
		pos = AdvancePosition(pos, "*/")
		input = input[end+2:]

		if isTemplateDirective(raw) {
			flushText(commentStart)
			tokens.Add(Token{Kind: Directive, Value: raw, Span: Span{Start: commentStart, End: pos}})
			textPos = pos
			continue
		}

		writeBuilderString(&textBuf, fullComment)
	}

	flushText(pos)
	return tokens, nil
}

func isTemplateDirective(s string) bool {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "%") {
		return false
	}
	s = strings.TrimSpace(strings.TrimPrefix(s, "%"))
	return s == "where" || s == "set" || s == "end" || strings.HasPrefix(s, "if ")
}

func writeBuilderString(builder *strings.Builder, value string) {
	if _, err := builder.WriteString(value); err != nil {
		panic(err)
	}
}
