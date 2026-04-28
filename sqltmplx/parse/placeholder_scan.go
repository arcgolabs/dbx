package parse

import "unicode"

func skipIdentifierExpr(input string, start int) (int, error) {
	i := start
	for i < len(input) && isIdentifierPart(rune(input[i])) {
		i++
	}
	return skipExprSuffixes(input, i)
}

func skipExprSuffixes(input string, start int) (int, error) {
	i := start
	for {
		next, advanced, err := nextExprSuffix(input, i)
		if err != nil {
			return 0, err
		}
		if !advanced {
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
		case '\'':
			next, err := skipQuoted(input, i)
			if err != nil {
				return 0, err
			}
			i = next - 1
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
		switch input[i] {
		case ' ', '\t', '\n', '\r', ',', ')':
			if i == start {
				return 0, errEmptyScalarLiteral
			}
			return i, nil
		}
		i++
	}
	if i == start {
		return 0, errEmptyScalarLiteral
	}
	return i, nil
}

func nextExprSuffix(input string, start int) (int, bool, error) {
	i := skipSpaces(input, start)
	if i >= len(input) {
		return start, false, nil
	}
	if next, ok, err := skipIdentifierCall(input, i); ok || err != nil {
		return next, ok, err
	}
	if next, ok, err := skipCastSuffix(input, i); ok || err != nil {
		return next, ok, err
	}
	if next, ok, err := skipIndexSuffix(input, i); ok || err != nil {
		return next, ok, err
	}
	return start, false, nil
}

func skipIdentifierCall(input string, start int) (int, bool, error) {
	if start >= len(input) || input[start] != '.' {
		return start, false, nil
	}
	i := start + 1
	if i >= len(input) || !isIdentifierStart(rune(input[i])) {
		return start, false, nil
	}
	i++
	for i < len(input) && isIdentifierPart(rune(input[i])) {
		i++
	}
	if i < len(input) && input[i] == '(' {
		next, err := skipBalanced(input, i, '(', ')')
		return next, true, err
	}
	return i, true, nil
}

func skipCastSuffix(input string, start int) (int, bool, error) {
	if !hasCastPrefix(input, start) {
		return start, false, nil
	}
	i := start + 2
	i = skipSpaces(input, i)
	end := scanCastTypeSuffix(input, i)
	if end == i {
		return 0, true, errInvalidCastSuffix
	}
	if hasArraySuffix(input, end) {
		end += 2
	}
	return end, true, nil
}

func skipIndexSuffix(input string, start int) (int, bool, error) {
	if start >= len(input) || input[start] != '[' {
		return start, false, nil
	}
	next, err := skipBalanced(input, start, '[', ']')
	return next, true, err
}

func skipSpaces(input string, start int) int {
	i := start
	for i < len(input) && (input[i] == ' ' || input[i] == '\t' || input[i] == '\n' || input[i] == '\r') {
		i++
	}
	return i
}

func hasCastPrefix(input string, start int) bool {
	return start+1 < len(input) && input[start] == ':' && input[start+1] == ':'
}

func scanCastTypeSuffix(input string, start int) int {
	i := start
	for i < len(input) && isCastIdentifierChar(rune(input[i])) {
		i++
	}
	return i
}

func isCastIdentifierChar(r rune) bool {
	return r == '_' || r == '.' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func hasArraySuffix(input string, start int) bool {
	return start+1 < len(input) && input[start] == '[' && input[start+1] == ']'
}
