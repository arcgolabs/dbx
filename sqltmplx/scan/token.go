package scan

// Kind identifies the type of scanned token.
type Kind int

const (
	// Text marks plain SQL text.
	Text Kind = iota + 1
	// Directive marks a template directive comment.
	Directive
)

// Token represents a scanned template token.
type Token struct {
	Kind  Kind
	Value string
}
