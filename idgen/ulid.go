package idgen

import (
	"context"

	"github.com/oklog/ulid/v2"
)

type ulidGenerator struct{}

func NewULID() Generator {
	return ulidGenerator{}
}

func (ulidGenerator) GenerateID(_ context.Context, request Request) (any, error) {
	if request.Strategy != StrategyULID {
		return nil, unsupportedStrategy(request.Strategy)
	}
	return ulid.Make().String(), nil
}
