package render

import (
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

func defaultExprRun(program *vm.Program, env any) (any, error) {
	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("run expr: %w", err)
	}
	return result, nil
}
