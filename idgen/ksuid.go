package idgen

import (
	"context"

	"github.com/segmentio/ksuid"
)

type ksuidGenerator struct{}

func NewKSUID() Generator {
	return ksuidGenerator{}
}

func (ksuidGenerator) GenerateID(_ context.Context, request Request) (any, error) {
	if request.Strategy != StrategyKSUID {
		return nil, unsupportedStrategy(request.Strategy)
	}
	return ksuid.New().String(), nil
}
