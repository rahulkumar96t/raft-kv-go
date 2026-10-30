package kv

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Submitter is anything that can execute an Op: a *Server in-process, or a
// stub in tests.
type Submitter interface {
	Submit(ctx context.Context, op Op) (Result, error)
}

// Clerk is a client that finds the leader and retries until a request
// succeeds. Retries reuse the request's sequence number, so the cluster
// applies each request at most once. A Clerk issues one request at a time
// and is not safe for concurrent use; use one Clerk per goroutine.
type Clerk struct {
	servers  []Submitter // servers[i] hosts Raft node i+1
	clientID string
	seq      uint64
	leader   int
	rng      *rand.Rand

	// AttemptTimeout bounds each attempt against one server.
	AttemptTimeout time.Duration
	// LoseReplyRate discards successful replies with this probability and
	// retries, simulating a lossy client network (exercises de-duplication).
	LoseReplyRate float64
	// Retries counts attempts beyond the first, summed over all requests.
	Retries int
}

// NewClerk returns a clerk; servers[i] must host Raft node i+1.
func NewClerk(servers []Submitter, clientID string, seed uint64) *Clerk {
	return &Clerk{
		servers:        servers,
		clientID:       clientID,
		rng:            rand.New(rand.NewPCG(seed, 1)),
		AttemptTimeout: 500 * time.Millisecond,
	}
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, Op{Kind: OpPut, Key: key, Value: value})
	return err
}

// Get returns the value of key and whether it exists.
func (c *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	res, err := c.do(ctx, Op{Kind: OpGet, Key: key})
	return res.Value, res.Found, err
}

// Delete removes key and reports whether it existed.
func (c *Clerk) Delete(ctx context.Context, key string) (bool, error) {
	res, err := c.do(ctx, Op{Kind: OpDelete, Key: key})
	return res.Found, err
}

func (c *Clerk) do(ctx context.Context, op Op) (Result, error) {
	c.seq++
	op.ClientID, op.Seq = c.clientID, c.seq
	backoff, hinted := 5*time.Millisecond, false
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if attempt > 0 {
			c.Retries++
		}
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		res, err := c.servers[c.leader].Submit(actx, op)
		cancel()
		if err == nil {
			if c.LoseReplyRate > 0 && c.rng.Float64() < c.LoseReplyRate {
				continue
			}
			return res, nil
		}
		var nl *NotLeaderError
		if errors.As(err, &nl) && nl.Leader >= 1 && nl.Leader <= len(c.servers) && nl.Leader-1 != c.leader && !hinted {
			// Follow the hint right away, but only once per back-off round
			// so two stale hints cannot bounce us around in a tight loop.
			c.leader, hinted = nl.Leader-1, true
			continue
		}
		c.leader = (c.leader + 1) % len(c.servers)
		// Back off (5 ms doubling to 100 ms) so a cluster that is electing a
		// leader is not hammered.
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff, hinted = min(2*backoff, 100*time.Millisecond), false
	}
}
