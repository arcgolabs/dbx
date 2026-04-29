// Package idgen provides ID generator strategies for database-oriented services.
package idgen

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/mapping"
)

type defaultGenerator struct {
	generators *collectionx.Map[Strategy, Generator]
}

func NewDefault(nodeID uint16) (Generator, error) {
	snowflake, err := NewSnowflake(nodeID)
	if err != nil {
		return nil, err
	}

	generators := collectionx.NewMapWithCapacity[Strategy, Generator](4)
	generators.Set(StrategySnowflake, snowflake)
	generators.Set(StrategyUUID, NewUUID())
	generators.Set(StrategyULID, NewULID())
	generators.Set(StrategyKSUID, NewKSUID())

	return &defaultGenerator{generators: generators}, nil
}

func (g *defaultGenerator) GenerateID(ctx context.Context, request Request) (any, error) {
	if g == nil {
		return nil, unsupportedStrategy(request.Strategy)
	}
	generator, ok := g.generators.Get(request.Strategy)
	if !ok {
		return nil, unsupportedStrategy(request.Strategy)
	}
	id, err := generator.GenerateID(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("dbx/idgen: generate %s id: %w", request.Strategy, err)
	}
	return id, nil
}
