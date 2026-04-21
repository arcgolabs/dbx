package sqltmplx

import "errors"

var (
	// ErrSpreadParamEmpty reports an empty spread parameter.
	ErrSpreadParamEmpty = errors.New("sqltmplx: spread parameter is empty")
	// ErrSpreadParamType reports an unsupported spread parameter type.
	ErrSpreadParamType = errors.New("sqltmplx: spread parameter must be slice or array")
)
