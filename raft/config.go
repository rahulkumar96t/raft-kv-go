package raft

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Config is the static configuration of a node. Zero durations fall back to
// defaults that suit a local cluster (50 ms heartbeats, 300-600 ms elections).
type Config struct {
	// ID identifies this node; IDs must be >= 1 (0 means "nobody").
	ID int
	// Peers lists the IDs of all cluster members, including ID.
	Peers []int

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// RPCTimeout bounds every outgoing RPC. Keep it well below
	// ElectionTimeoutMin so a slow peer cannot starve heartbeats.
	RPCTimeout time.Duration
	// TickInterval is the resolution of the election and heartbeat timers.
	TickInterval time.Duration
	// MaxEntriesPerRPC caps the entries carried by one AppendEntries call.
	MaxEntriesPerRPC int

	// Seed seeds the election-timeout jitter; 0 derives one from the clock.
	Seed int64
	// Logger receives election and replication events; nil discards them.
	Logger *slog.Logger
	// OnLeader, if set, is called whenever this node becomes leader. It runs
	// with the node's lock held, so it must be fast and must not call back
	// into the node. Tests use it to assert Election Safety.
	OnLeader func(id int, term uint64)
}

func (c Config) withDefaults() Config {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 100 * time.Millisecond
	}
	if c.TickInterval == 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.MaxEntriesPerRPC == 0 {
		c.MaxEntriesPerRPC = 128
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return c
}

func (c Config) validate() error {
	if c.ID < 1 {
		return fmt.Errorf("raft: node ID must be >= 1, got %d", c.ID)
	}
	seen := map[int]bool{}
	for _, p := range c.Peers {
		if p < 1 {
			return fmt.Errorf("raft: peer IDs must be >= 1, got %d", p)
		}
		if seen[p] {
			return fmt.Errorf("raft: duplicate peer ID %d", p)
		}
		seen[p] = true
	}
	if !seen[c.ID] {
		return fmt.Errorf("raft: Peers must include the node's own ID %d", c.ID)
	}
	if c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return errors.New("raft: ElectionTimeoutMax must be >= ElectionTimeoutMin")
	}
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return errors.New("raft: HeartbeatInterval must be shorter than ElectionTimeoutMin")
	}
	if c.MaxEntriesPerRPC < 1 {
		return errors.New("raft: MaxEntriesPerRPC must be positive")
	}
	return nil
}
