package render

import (
	"strings"
	"unicode"
)

func skipIdentifierExpr(input string, start int) (int, error) {
	i := start + 1
	for i < len(input) && isIdentifierPart(rune(input[i])) {
		i++
	}
	for {
		next, handled, err := skipIdentifierCall(input, i)
		if err != nil {
			return 0, err
		}
		if !handled {
			return i, nil
		}
		i = next
	}
}

func skipExprSuffixes(input string, start int) (int, error) {
	i := start
	for {
		next, handled, err := nextExprSuffix(input, i)
		if err != nil {
			return 0, err
		}
		if !handled {
			return i, nil
		}
		i = next
	}
}

func isIdentifierStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentifierPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func skipBalanced(input string, start int, open, closeByte byte) (int, error) {
	depth := 0
	for i := start; i < len(input); i++ {
		switch input[i] {
		case '\'', '"':
			j, err := skipQuoted(input, i)
			if err != nil {
				return 0, err
			}
			i = j - 1
		case open:
			depth++
		case closeByte:
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return 0, errUnterminatedBalanced
}

func skipQuoted(input string, start int) (int, error) {
	quote := input[start]
	for i := start + 1; i < len(input); i++ {
		if input[i] != quote {
			continue
		}
		if i+1 < len(input) && input[i+1] == quote {
			i++
			continue
		}
		return i + 1, nil
	}
	return 0, errUnterminatedQuoted
}

func skipScalarToken(input string, start int) (int, error) {
	i := start
	for i < len(input) {
		r := rune(input[i])
		if unicode.IsSpace(r) || r == ',' || r == ')' || r == '(' || r == ']' {
			break
		}
		i++
	}
	if i == start {
		return 0, errEmptyScalarLiteral
	}
	return i, nil
}

func skipIdentifierCall(input string, start int) (int, bool, error) {
	next := skipSpaces(input, start)
	if next >= len(input) || input[next] != '(' {
		return 0, false, nil
	}
	index, err := skipBalanced(input, next, '(', ')')
	if err != nil {
		return 0, false, err
	}
	return index, true, nil
}

func skipCastSuffix(input string, start int) (int, bool, error) {
	if !hasCastPrefix(input, start) {
		return 0, false, nil
	}

	index := start + 2
	if index >= len(input) || !isIdentifierStart(rune(input[index])) {
		return 0, false, errInvalidCastSuffix
	}
	return scanCastTypeSuffix(input, index+1), true, nil
}

func skipIndexSuffix(input string, start int) (int, bool, error) {
	if start >= len(input) || input[start] != '[' {
		return 0, false, nil
	}
	index, err := skipBalanced(input, start, '[', ']')
	if err != nil {
		return 0, false, err
	}
	return index, true, nil
}

func skipSpaces(input string, start int) int {
	index := start
	for index < len(input) && unicode.IsSpace(rune(input[index])) {
		index++
	}
	return index
}

func writeBuilderByte(builder *strings.Builder, value byte) {
	if err := builder.WriteByte(value); err != nil {
		panic(err)
	}
}

func nextExprSuffix(input string, start int) (int, bool, error) {
	next := skipSpaces(input, start)
	if castNext, handled, err := skipCastSuffix(input, next); handled || err != nil {
		return castNext, handled, err
	}
	return skipIndexSuffix(input, next)
}

func hasCastPrefix(input string, start int) bool {
	return start+1 < len(input) && input[start] == ':' && input[start+1] == ':'
}

func scanCastTypeSuffix(input string, start int) int {
	index := start
	for index < len(input) {
		if isCastIdentifierChar(rune(input[index])) {
			index++
			continue
		}
		if hasArraySuffix(input, index) {
			index += 2
			continue
		}
		break
	}
	return index
}

func isCastIdentifierChar(r rune) bool {
	return isIdentifierPart(r) || r == '.'
}

func hasArraySuffix(input string, start int) bool {
	return start+1 < len(input) && input[start] == '[' && input[start+1] == ']'
}
