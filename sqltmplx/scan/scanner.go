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
	)

	flushText := func() {
		if textBuf.Len() == 0 {
			return
		}
		tokens.Add(Token{Kind: Text, Value: textBuf.String()})
		textBuf.Reset()
	}

	for input != "" {
		start := strings.Index(input, "/*")
		if start < 0 {
			writeBuilderString(&textBuf, input)
			break
		}

		writeBuilderString(&textBuf, input[:start])
		input = input[start+2:]

		end := strings.Index(input, "*/")
		if end < 0 {
			return nil, errors.New("sqltmplx: unterminated directive comment")
		}

		rawBody := input[:end]
		raw := strings.TrimSpace(rawBody)
		fullComment := "/*" + rawBody + "*/"
		input = input[end+2:]

		if isTemplateDirective(raw) {
			flushText()
			tokens.Add(Token{Kind: Directive, Value: raw})
			continue
		}

		writeBuilderString(&textBuf, fullComment)
	}

	flushText()
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
