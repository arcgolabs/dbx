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

// NewTyped creates a typed statement source from a name and typed bind function.
func NewTyped[P any](name string, binder func(P) (Bound, error)) TypedSource[P] {
	if binder == nil {
		return For[P](New(name, nil))
	}
	return For[P](New(name, func(params any) (Bound, error) {
		value, ok := params.(P)
		if !ok {
			return Bound{}, fmt.Errorf("dbx/sqlstmt: typed statement %q params type mismatch", name)
		}
		return binder(value)
	}))
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
