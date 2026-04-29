package sqlstmt

import "fmt"

// TypedSource binds a concrete parameter type into a SQL statement source.
type TypedSource[P any] struct {
	source Source
}

// For returns a typed view of source. It keeps SQL rendering dynamic internally
// while making call sites pass a concrete parameter type.
func For[P any](source Source) TypedSource[P] {
	return TypedSource[P]{source: source}
}

func (s TypedSource[P]) StatementName() string {
	return Name(s.source)
}

func (s TypedSource[P]) Bind(params P) (Bound, error) {
	if s.source == nil {
		return Bound{}, ErrNilStatement
	}
	bound, err := s.source.Bind(params)
	if err != nil {
		return Bound{}, fmt.Errorf("bind typed statement: %w", err)
	}
	return bound, nil
}

func (s TypedSource[P]) Source() Source {
	return s.source
}
