package kv_test

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/kv"
	"github.com/rahulkumar96t/raft-kv-go/raft"
	"github.com/rahulkumar96t/raft-kv-go/transport/simnet"
)

// cluster runs n kv.Servers on a simulated network. Clients reach servers
// through slots, so a crashed server simply refuses requests and a restarted
// one takes over its slot.
type cluster struct {
	t         *testing.T
	n         int
	seed      int64
	threshold int
	net       *simnet.Network

	mu         sync.Mutex
	servers    []*kv.Server
	persisters []*raft.MemoryPersister
	slots      []*slot
}

type slot struct{ srv atomic.Pointer[kv.Server] }

func (s *slot) Submit(ctx context.Context, op kv.Op) (kv.Result, error) {
	srv := s.srv.Load()
	if srv == nil {
		return kv.Result{}, kv.ErrStopped
	}
	return srv.Submit(ctx, op)
}

func newCluster(t *testing.T, n, snapshotThreshold int) *cluster {
	t.Helper()
	h := fnv.New64a()
	h.Write([]byte(t.Name()))
	seed := int64(h.Sum64() >> 1)
	c := &cluster{
		t:          t,
		n:          n,
		seed:       seed,
		threshold:  snapshotThreshold,
		net:        simnet.New(simnet.Options{Seed: seed}),
		servers:    make([]*kv.Server, n+1),
		persisters: make([]*raft.MemoryPersister, n+1),
		slots:      make([]*slot, n+1),
	}
	for id := 1; id <= n; id++ {
		c.persisters[id] = raft.NewMemoryPersister()
		c.slots[id] = &slot{}
		c.start(id)
	}
	t.Cleanup(func() {
		for id := 1; id <= n; id++ {
			c.crash(id)
		}
	})
	return c
}

func (c *cluster) start(id int) {
	peers := make([]int, c.n)
	for i := range peers {
		peers[i] = i + 1
	}
	c.mu.Lock()
	p := c.persisters[id]
	c.mu.Unlock()
	srv, err := kv.NewServer(kv.Config{
		Raft: raft.Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			RPCTimeout:         100 * time.Millisecond,
			Seed:               c.seed,
		},
		SnapshotThreshold: c.threshold,
	}, c.net.Transport(id), p)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.servers[id] = srv
	c.mu.Unlock()
	c.net.Register(id, srv.Node())
	c.slots[id].srv.Store(srv)
}

func (c *cluster) crash(id int) {
	c.net.Disconnect(id)
	c.slots[id].srv.Store(nil)
	c.mu.Lock()
	srv := c.servers[id]
	c.servers[id] = nil
	c.mu.Unlock()
	if srv == nil {
		return
	}
	srv.Stop()
	c.mu.Lock()
	c.persisters[id] = c.persisters[id].Copy()
	c.mu.Unlock()
}

func (c *cluster) server(id int) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

// clerk returns a client that may talk to the given nodes (all if none).
func (c *cluster) clerk(name string, ids ...int) *kv.Clerk {
	if len(ids) == 0 {
		for id := 1; id <= c.n; id++ {
			ids = append(ids, id)
		}
	}
	subs := make([]kv.Submitter, c.n)
	for i := range subs {
		subs[i] = refuse{}
	}
	for _, id := range ids {
		subs[id-1] = c.slots[id]
	}
	h := fnv.New64a()
	h.Write([]byte(name))
	return kv.NewClerk(subs, name, h.Sum64())
}

// refuse stands in for servers a clerk is not allowed to reach.
type refuse struct{}

func (refuse) Submit(context.Context, kv.Op) (kv.Result, error) { return kv.Result{}, kv.ErrStopped }

// leader waits until some server accepts a no-op read as leader.
func (c *cluster) leader() int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id := 1; id <= c.n; id++ {
			if srv := c.server(id); srv != nil && srv.Node().Status().Role == "leader" {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
