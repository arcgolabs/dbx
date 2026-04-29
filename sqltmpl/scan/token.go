package scan

import "unicode/utf8"

// Kind identifies the type of scanned token.
type Kind int

const (
	// Text marks plain SQL text.
	Text Kind = iota + 1
	// Directive marks a template directive comment.
	Directive
)

// Position identifies a byte offset and human-readable line/column location.
type Position struct {
	Offset int
	Line   int
	Column int
}

// Span identifies a source range in the template.
type Span struct {
	Start Position
	End   Position
}

// Token represents a scanned template token.
type Token struct {
	Kind  Kind
	Value string
	Span  Span
}

// AdvancePosition returns the position reached after consuming value from start.
func AdvancePosition(start Position, value string) Position {
	current := start
	for _, r := range value {
		current.Offset += utf8.RuneLen(r)
		if r == '\n' {
			current.Line++
			current.Column = 1
			continue
		}
		current.Column++
	}
	return current
}
