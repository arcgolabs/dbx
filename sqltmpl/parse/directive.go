package parse

// Directive represents a parsed template directive.
type Directive struct {
	If    *IfDirective
	Where *WhereDirective
	Set   *SetDirective
	End   *EndDirective
}

// IfDirective represents an `% if ...` directive.
type IfDirective struct {
	Keyword string
	Expr    string
}

// WhereDirective represents a `% where` directive.
type WhereDirective struct {
	Keyword string
}

// SetDirective represents a `% set` directive.
type SetDirective struct {
	Keyword string
}

// EndDirective represents a `% end` directive.
type EndDirective struct {
	Keyword string
}
