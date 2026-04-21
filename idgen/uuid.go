package idgen

import (
	"context"
	"fmt"

	"github.com/google/uuid"
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
		id, err := uuid.NewV7()
		if err != nil {
			return "", fmt.Errorf("dbx/idgen: generate uuid v7: %w", err)
		}
		return id.String(), nil
	case "v4":
		return uuid.NewString(), nil
	default:
		return "", fmt.Errorf("dbx/idgen: unsupported uuid version %q", version)
	}
}
