package idgen

import (
	"context"
	"sync"
	"time"
)

type snowflakeGenerator struct {
	mu           sync.Mutex
	nodeID       uint16
	lastUnixMs   int64
	snowflakeSeq int64
}

func NewSnowflake(nodeID uint16) (Generator, error) {
	if nodeID < MinNodeID || nodeID > MaxNodeID {
		return nil, &NodeIDOutOfRangeError{NodeID: nodeID, Min: MinNodeID, Max: MaxNodeID}
	}
	return &snowflakeGenerator{nodeID: nodeID}, nil
}

func (g *snowflakeGenerator) GenerateID(_ context.Context, request Request) (any, error) {
	if request.Strategy != StrategySnowflake {
		return nil, unsupportedStrategy(request.Strategy)
	}
	return g.nextID(), nil
}

func (g *snowflakeGenerator) nextID() int64 {
	const sequenceMask int64 = (1 << 12) - 1

	g.mu.Lock()
	defer g.mu.Unlock()

	nowMs := time.Now().UnixMilli()
	if nowMs == g.lastUnixMs {
		g.snowflakeSeq = (g.snowflakeSeq + 1) & sequenceMask
		if g.snowflakeSeq == 0 {
			for nowMs <= g.lastUnixMs {
				nowMs = time.Now().UnixMilli()
			}
		}
	} else {
		g.snowflakeSeq = 0
	}
	g.lastUnixMs = nowMs

	// 41-bit timestamp + 10-bit node id + 12-bit sequence.
	return (nowMs << 22) | (int64(g.nodeID) << 12) | g.snowflakeSeq
}
