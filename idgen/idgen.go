package idgen

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
)

type Strategy string

type Request struct {
	Strategy    Strategy
	UUIDVersion string
}

type Generator interface {
	GenerateID(ctx context.Context, request Request) (any, error)
}

const (
	StrategyUnset     Strategy = ""
	StrategyDBAuto    Strategy = "db_auto"
	StrategySnowflake Strategy = "snowflake"
	StrategyUUID      Strategy = "uuid"
	StrategyULID      Strategy = "ulid"
	StrategyKSUID     Strategy = "ksuid"

	DefaultUUIDVersion = "v7"

	DefaultNodeID uint16 = 1
	MinNodeID     uint16 = 1
	MaxNodeID     uint16 = 1023
)

var ErrInvalidNodeID = errors.New("dbx/idgen: node id is out of range")

func ResolveNodeIDFromHostName() uint16 {
	hostName, err := os.Hostname()
	if err != nil || hostName == "" {
		return DefaultNodeID
	}
	hasher := fnv.New32a()
	if _, err := hasher.Write([]byte(hostName)); err != nil {
		return DefaultNodeID
	}
	id := uint16(hasher.Sum32() % (uint32(MaxNodeID) + 1))
	if id < MinNodeID {
		return MinNodeID
	}
	return id
}

type NodeIDOutOfRangeError struct {
	NodeID uint16
	Min    uint16
	Max    uint16
}

func (e *NodeIDOutOfRangeError) Error() string {
	return fmt.Sprintf("dbx/idgen: node id %d out of range [%d,%d]", e.NodeID, e.Min, e.Max)
}

func (e *NodeIDOutOfRangeError) Unwrap() error {
	return ErrInvalidNodeID
}

func unsupportedStrategy(strategy Strategy) error {
	return fmt.Errorf("dbx/idgen: unsupported id strategy %q", strategy)
}
