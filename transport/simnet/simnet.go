// Package simnet is an in-memory network for testing Raft clusters. It can
// disconnect and partition nodes and drop, delay and reorder messages.
//
// Every message is JSON-encoded on the way in and out, exactly like the HTTP
// transport, so nodes never share memory and an RPC type that does not
// survive serialisation fails in tests too.
//
// All fault decisions come from one seeded generator, so the same seed yields
// the same sequence of decisions. Which RPC draws which decision still depends
// on goroutine scheduling, so whole runs are reproducible only in spirit.
package simnet

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/raft"
)

// Options configures the fault model. The zero value is a reliable network
// with no delay.
type Options struct {
	// Seed seeds the fault generator (only read by New).
	Seed int64
	// DropRate is the probability that a request, or independently its
	// reply, is lost. A lost message looks like a timeout to the sender.
	DropRate float64
	// MinDelay and MaxDelay bound the one-way latency of each message.
	MinDelay, MaxDelay time.Duration
	// ReorderRate is the probability that a message gets an extra delay of
	// up to ReorderDelay, which makes it overtake-able by later messages.
	ReorderRate  float64
	ReorderDelay time.Duration
}

// Stats counts what happened to RPCs.
type Stats struct {
	Calls       int64 // RPCs attempted
	Delivered   int64 // replies that reached the caller's goroutine
	Dropped     int64 // RPCs whose request or reply was lost
	Unreachable int64 // RPCs blocked by a disconnect or partition
}

// Network connects registered RPC handlers.
type Network struct {
	mu       sync.Mutex
	opts     Options
	rng      *rand.Rand
	handlers map[int]raft.RPCHandler
	down     map[int]bool
	group    map[int]int // partition group; equal groups can talk
	stats    Stats
}

// New returns a network using opts as its initial fault model.
func New(opts Options) *Network {
	return &Network{
		opts:     opts,
		rng:      rand.New(rand.NewPCG(uint64(opts.Seed), 0x5eed)),
		handlers: map[int]raft.RPCHandler{},
		down:     map[int]bool{},
		group:    map[int]int{},
	}
}

// SetOptions replaces the fault model, keeping the random generator.
func (n *Network) SetOptions(opts Options) {
	n.mu.Lock()
	defer n.mu.Unlock()
	opts.Seed = n.opts.Seed
	n.opts = opts
}

// Register attaches (or replaces, after a restart) the handler for id and
// connects it.
func (n *Network) Register(id int, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	delete(n.down, id)
}

// Disconnect cuts id off from everyone.
func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = true
}

// Connect undoes Disconnect.
func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.down, id)
}

// Partition splits the network: nodes can only talk within their group.
// Registered nodes not listed in any group are isolated.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	for id := range n.handlers {
		n.group[id] = -id
	}
	for g, members := range groups {
		for _, id := range members {
			n.group[id] = g + 1
		}
	}
}

// Heal removes all partitions (but not disconnects).
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
}

// Stats returns a copy of the counters.
func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// Transport returns the raft.Transport used by node from.
func (n *Network) Transport(from int) raft.Transport { return endpoint{net: n, from: from} }

func (n *Network) linkUpLocked(from, to int) bool {
	if n.down[from] || n.down[to] {
		return false
	}
	if _, ok := n.handlers[to]; !ok {
		return false
	}
	return n.group[from] == n.group[to]
}

// handlerIfUp returns to's handler if from can currently reach it.
func (n *Network) handlerIfUp(from, to int) raft.RPCHandler {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.linkUpLocked(from, to) {
		return nil
	}
	return n.handlers[to]
}

// roll draws the fate of one message.
func (n *Network) roll() (drop bool, delay time.Duration) {
	o := n.opts
	if o.DropRate > 0 && n.rng.Float64() < o.DropRate {
		return true, 0
	}
	delay = o.MinDelay
	if o.MaxDelay > o.MinDelay {
		delay += time.Duration(n.rng.Int64N(int64(o.MaxDelay - o.MinDelay)))
	}
	if o.ReorderRate > 0 && n.rng.Float64() < o.ReorderRate {
		delay += time.Duration(n.rng.Int64N(int64(o.ReorderDelay) + 1))
	}
	return false, delay
}

// call performs one RPC. Delivery happens on its own goroutine, so a request
// can still reach the receiver after the caller has given up: late and
// reordered messages are part of the model.
func call[A, R any](ctx context.Context, n *Network, from, to int, args *A, invoke func(raft.RPCHandler, *A) (*R, error)) (*R, error) {
	wire, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.stats.Calls++
	up := n.linkUpLocked(from, to)
	reqDrop, reqDelay := n.roll()
	repDrop, repDelay := n.roll()
	switch {
	case !up:
		n.stats.Unreachable++
	case reqDrop || repDrop:
		n.stats.Dropped++
	}
	n.mu.Unlock()

	if !up || reqDrop {
		<-ctx.Done() // the sender only finds out by timing out
		return nil, fmt.Errorf("simnet: %d -> %d: %w", from, to, ctx.Err())
	}

	out := make(chan *R, 1)
	go func() {
		time.Sleep(reqDelay)
		h := n.handlerIfUp(from, to)
		if h == nil {
			return
		}
		var a A
		if json.Unmarshal(wire, &a) != nil {
			return
		}
		reply, err := invoke(h, &a)
		if err != nil {
			return // e.g. the receiver was stopped: no reply
		}
		time.Sleep(repDelay)
		if repDrop || n.handlerIfUp(to, from) == nil {
			return
		}
		b, err := json.Marshal(reply)
		if err != nil {
			return
		}
		var r R
		if json.Unmarshal(b, &r) != nil {
			return
		}
		n.mu.Lock()
		n.stats.Delivered++
		n.mu.Unlock()
		out <- &r
	}()

	select {
	case r := <-out:
		return r, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("simnet: %d -> %d: %w", from, to, ctx.Err())
	}
}

type endpoint struct {
	net  *Network
	from int
}

func (e endpoint) RequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(ctx, e.net, e.from, to, args, raft.RPCHandler.HandleRequestVote)
}

func (e endpoint) AppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call(ctx, e.net, e.from, to, args, raft.RPCHandler.HandleAppendEntries)
}

func (e endpoint) InstallSnapshot(ctx context.Context, to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call(ctx, e.net, e.from, to, args, raft.RPCHandler.HandleInstallSnapshot)
}
