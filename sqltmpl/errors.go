package sqltmpl

import "errors"

var (
	// ErrSpreadParamEmpty reports an empty spread parameter.
	ErrSpreadParamEmpty = errors.New("sqltmpl: spread parameter is empty")
	// ErrSpreadParamType reports an unsupported spread parameter type.
	ErrSpreadParamType = errors.New("sqltmpl: spread parameter must be slice or array")
)
