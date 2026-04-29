package render

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

var (
	errSpreadParamEmpty       = errors.New("sqltmpl: spread parameter is empty")
	errSpreadParamType        = errors.New("sqltmpl: spread parameter must be slice or array")
	errUnterminatedSQLComment = errors.New("sqltmpl: unterminated sql comment")
	errInvalidCastSuffix      = errors.New("invalid cast suffix")
	errUnterminatedBalanced   = errors.New("unterminated balanced literal")
	errUnterminatedQuoted     = errors.New("unterminated quoted literal")
	errEmptyScalarLiteral     = errors.New("empty scalar literal")
)

func bindText(input string, st *state) (string, error) {
	var out strings.Builder
	out.Grow(len(input))
	for i := 0; i < len(input); {
		if i+1 < len(input) && input[i] == '/' && input[i+1] == '*' {
			text, next, handled, err := bindCommentPlaceholder(input, i, st)
			if err != nil {
				return "", err
			}
			if handled {
				writeBuilderString(&out, text)
				i = next
				continue
			}
		}
		writeBuilderByte(&out, input[i])
		i++
	}
	return out.String(), nil
}

func bindCommentPlaceholder(input string, i int, st *state) (string, int, bool, error) {
	raw, commentEnd, handled, err := parseCommentPlaceholder(input, i)
	if err != nil || !handled {
		return "", commentEnd, handled, err
	}

	sampleStart, err := placeholderSampleStart(input, commentEnd, raw)
	if err != nil {
		return "", 0, false, err
	}

	spread := input[sampleStart] == '(' || looksLikeCollectionSample(input, sampleStart)
	text, err := bindParam(raw, spread, st)
	if err != nil {
		return "", 0, false, err
	}

	k, err := skipPlaceholderSample(input, sampleStart)
	if err != nil {
		return "", 0, false, fmt.Errorf("sqltmpl: placeholder %q invalid test literal: %w", raw, err)
	}
	return text, k, true, nil
}

func bindParam(name string, spread bool, st *state) (string, error) {
	val, err := lookupParam(st.params, name)
	if err != nil {
		return "", err
	}
	if !spread {
		st.args.Add(val)
		return st.nextBind(), nil
	}

	return bindSpreadParam(val, st)
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

func lookupParam(params any, name string) (any, error) {
	val, ok := lookupValue(params, name)
	if !ok {
		return nil, fmt.Errorf("sqltmpl: parameter %q not found", name)
	}
	return val, nil
}

func bindSpreadParam(val any, st *state) (string, error) {
	rv, err := spreadValue(val)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	length := rv.Len()
	out.Grow(length * 4)
	for j := range length {
		appendSpreadBind(&out, j, st.nextBind())
		st.args.Add(rv.Index(j).Interface())
	}
	return out.String(), nil
}

func spreadValue(val any) (reflect.Value, error) {
	rv := reflect.ValueOf(val)
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, errSpreadParamEmpty
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return reflect.Value{}, errSpreadParamEmpty
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return reflect.Value{}, errSpreadParamType
	}
	if rv.Len() == 0 {
		return reflect.Value{}, errSpreadParamEmpty
	}
	return rv, nil
}

func appendSpreadBind(out *strings.Builder, index int, bind string) {
	if index > 0 {
		writeBuilderString(out, ", ")
	}
	writeBuilderString(out, bind)
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
