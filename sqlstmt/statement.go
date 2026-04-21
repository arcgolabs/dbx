package sqlstmt

import "errors"

var ErrNilStatement = errors.New("dbx/sqlstmt: statement is nil")

// Source binds runtime parameters into executable SQL.
type Source interface {
	StatementName() string
	Bind(params any) (Bound, error)
}

// Name returns the statement name, or an empty string for nil sources.
func Name(source Source) string {
	if source == nil {
		return ""
	}
	return source.StatementName()
}

// Statement adapts a bind function into a reusable SQL statement source.
type Statement struct {
	name   string
	binder func(params any) (Bound, error)
}

// New creates a statement source from a name and bind function.
func New(name string, binder func(params any) (Bound, error)) *Statement {
	return &Statement{name: name, binder: binder}
}

func (s *Statement) StatementName() string {
	if s == nil {
		return ""
	}
	return s.name
}

func (s *Statement) Bind(params any) (Bound, error) {
	if s == nil || s.binder == nil {
		return Bound{}, ErrNilStatement
	}

	bound, err := s.binder(params)
	if err != nil {
		return Bound{}, err
	}
	if bound.Name == "" {
		bound.Name = s.name
	}
	if bound.Args.Len() > 0 {
		bound.Args = bound.Args.Clone()
	}
	return bound, nil
}
