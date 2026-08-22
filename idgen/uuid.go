package idgen

import (
	"context"
	"fmt"
	"uuid"
)

type uuidGenerator struct{}

func NewUUID() Generator {
	return uuidGenerator{}
}

func (uuidGenerator) GenerateID(_ context.Context, request Request) (any, error) {
	if request.Strategy != StrategyUUID {
		return nil, unsupportedStrategy(request.Strategy)
	}
	return nextUUID(request.UUIDVersion)
}

func nextUUID(version string) (string, error) {
	switch version {
	case "", "v7":
		return uuid.NewV7().String(), nil
	case "v4":
		return uuid.NewV4().String(), nil
	default:
		return "", fmt.Errorf("dbx/idgen: unsupported uuid version %q", version)
	}
}
