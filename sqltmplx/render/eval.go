package render

import (
	"errors"

	"github.com/expr-lang/expr/vm"
)

var exprRunner func(program *vm.Program, env any) (any, error)

var errExprRunnerNil = errors.New("sqltmplx: expr runner is nil")

func init() {
	exprRunner = defaultExprRun
}

func exprRun(program *vm.Program, env any) (any, error) {
	if exprRunner == nil {
		return nil, errExprRunnerNil
	}
	return exprRunner(program, env)
}
