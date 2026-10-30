package raft_test

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/raft"
	"github.com/rahulkumar96t/raft-kv-go/transport/simnet"
)

const (
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

// harness runs a cluster of real raft.Nodes on a simnet.Network and checks
// the safety properties continuously:
//
//   - Election Safety: at most one leader per term (via Config.OnLeader).
//   - State Machine Safety: no two nodes apply different commands at the
//     same index.
//   - every node applies indexes in order, without gaps (snapshots aside).
type harness struct {
	t             *testing.T
	n             int
	seed          int64
	net           *simnet.Network
	rng           *rand.Rand // test-side randomness (which node to crash, ...)
	snapshotEvery int

	mu         sync.Mutex
	nodes      []*raft.Node // nil while crashed
	persisters []*raft.MemoryPersister
	stops      []chan struct{}
	reachable  []bool   // can the test talk to it (not crashed or disconnected)
	applied    []int    // highest index applied per node
	snapshots  []int    // snapshots delivered per node since its last start
	committed  [][]byte // committed[i] = command applied at index i by anyone
	lastLeader int      // node that most recently accepted a proposal
	nextCmd    int

	failMu   sync.Mutex
	failures []string

	leaderMu sync.Mutex
	leaders  map[uint64]int // term -> leader
}

func testSeed(t *testing.T) int64 {
	if s := os.Getenv("RAFT_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad RAFT_SEED: %v", err)
		}
		return v
	}
	h := fnv.New64a()
	h.Write([]byte(t.Name()))
	return int64(h.Sum64() >> 1)
}

func newHarness(t *testing.T, n, snapshotEvery int) *harness {
	t.Helper()
	seed := testSeed(t)
	t.Logf("seed %d (override with RAFT_SEED)", seed)
	h := &harness{
		t:             t,
		n:             n,
		seed:          seed,
		net:           simnet.New(simnet.Options{Seed: seed}),
		rng:           rand.New(rand.NewPCG(uint64(seed), 7)),
		snapshotEvery: snapshotEvery,
		nodes:         make([]*raft.Node, n+1),
		persisters:    make([]*raft.MemoryPersister, n+1),
		stops:         make([]chan struct{}, n+1),
		reachable:     make([]bool, n+1),
		applied:       make([]int, n+1),
		snapshots:     make([]int, n+1),
		committed:     [][]byte{nil},
		leaders:       map[uint64]int{},
	}
	for id := 1; id <= n; id++ {
		h.persisters[id] = raft.NewMemoryPersister()
		h.start(id)
	}
	t.Cleanup(h.cleanup)
	return h
}

func (h *harness) cleanup() {
	for id := 1; id <= h.n; id++ {
		h.crash(id)
	}
	h.failMu.Lock()
	defer h.failMu.Unlock()
	for _, f := range h.failures {
		h.t.Error(f)
	}
}

func (h *harness) fail(format string, args ...any) {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	h.failures = append(h.failures, fmt.Sprintf(format, args...))
}

// checkInvariants stops the test early if a background check has failed.
func (h *harness) checkInvariants() {
	h.t.Helper()
	h.failMu.Lock()
	defer h.failMu.Unlock()
	if len(h.failures) > 0 {
		h.t.Fatalf("invariant violated: %s", h.failures[0])
	}
}

func (h *harness) config(id int) raft.Config {
	peers := make([]int, h.n)
	for i := range peers {
		peers[i] = i + 1
	}
	return raft.Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: electionTimeoutMin,
		ElectionTimeoutMax: electionTimeoutMax,
		HeartbeatInterval:  50 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
		MaxEntriesPerRPC:   32,
		Seed:               h.seed,
		OnLeader: func(id int, term uint64) {
			h.leaderMu.Lock()
			defer h.leaderMu.Unlock()
			if prev, ok := h.leaders[term]; ok && prev != id {
				h.fail("election safety: nodes %d and %d both led term %d", prev, id, term)
			}
			h.leaders[term] = id
		},
	}
}

// start boots node id from its persister.
func (h *harness) start(id int) {
	h.mu.Lock()
	p := h.persisters[id]
	h.applied[id], h.snapshots[id] = 0, 0
	stop := make(chan struct{})
	h.stops[id] = stop
	h.mu.Unlock()

	applyCh := make(chan raft.ApplyMsg)
	node, err := raft.New(h.config(id), h.net.Transport(id), p, applyCh)
	if err != nil {
		h.t.Fatal(err)
	}
	go h.consume(id, node, applyCh, stop)
	h.mu.Lock()
	h.nodes[id] = node
	h.reachable[id] = true
	h.mu.Unlock()
	h.net.Register(id, node)
}

// crash stops node id; its persisted state is kept for a later start.
func (h *harness) crash(id int) {
	h.net.Disconnect(id)
	h.mu.Lock()
	node, stop := h.nodes[id], h.stops[id]
	h.nodes[id], h.reachable[id] = nil, false
	h.mu.Unlock()
	if node == nil {
		return
	}
	node.Stop()
	close(stop)
	h.mu.Lock()
	h.persisters[id] = h.persisters[id].Copy()
	h.mu.Unlock()
}

func (h *harness) disconnect(id int) {
	h.net.Disconnect(id)
	h.mu.Lock()
	h.reachable[id] = false
	h.mu.Unlock()
}

func (h *harness) connect(id int) {
	h.net.Connect(id)
	h.mu.Lock()
	h.reachable[id] = h.nodes[id] != nil
	h.mu.Unlock()
}

func (h *harness) node(id int) *raft.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[id]
}

func (h *harness) isReachable(id int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reachable[id]
}

// consume plays the state machine for node id: it checks the order of
// applied entries and that they agree with every other node.
func (h *harness) consume(id int, node *raft.Node, applyCh <-chan raft.ApplyMsg, stop <-chan struct{}) {
	for {
		var msg raft.ApplyMsg
		select {
		case <-stop:
			return
		case msg = <-applyCh:
		}
		snapshotAt := 0
		h.mu.Lock()
		switch {
		case msg.SnapshotValid:
			if string(msg.Snapshot) != strconv.Itoa(msg.SnapshotIndex) {
				h.fail("node %d got snapshot %q for index %d", id, msg.Snapshot, msg.SnapshotIndex)
			}
			if msg.SnapshotIndex <= h.applied[id] {
				h.fail("node %d got snapshot at %d after applying %d", id, msg.SnapshotIndex, h.applied[id])
			}
			h.applied[id] = msg.SnapshotIndex
			h.snapshots[id]++
		case msg.CommandValid:
			idx := msg.CommandIndex
			if idx != h.applied[id]+1 {
				h.fail("node %d applied index %d right after %d", id, idx, h.applied[id])
			}
			for len(h.committed) <= idx {
				h.committed = append(h.committed, nil)
			}
			if prev := h.committed[idx]; prev != nil && !bytes.Equal(prev, msg.Command) {
				h.fail("state machine safety: index %d is %q on one node and %q on node %d", idx, prev, msg.Command, id)
			}
			cmd := msg.Command
			if cmd == nil {
				cmd = []byte{} // remember no-ops too
			}
			h.committed[idx] = cmd
			h.applied[id] = idx
			if h.snapshotEvery > 0 && idx%h.snapshotEvery == 0 {
				snapshotAt = idx
			}
		}
		h.mu.Unlock()
		if snapshotAt > 0 {
			node.Snapshot(snapshotAt, []byte(strconv.Itoa(snapshotAt)))
		}
	}
}

// nCommitted returns how many nodes have applied index, and the command there.
func (h *harness) nCommitted(index int) (int, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var cmd []byte
	if index < len(h.committed) {
		cmd = h.committed[index]
	}
	count := 0
	for id := 1; id <= h.n; id++ {
		if h.applied[id] >= index {
			count++
		}
	}
	return count, cmd
}

func (h *harness) wasCommitted(cmd []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.committed {
		if bytes.Equal(c, cmd) {
			return true
		}
	}
	return false
}

func (h *harness) newCmd() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextCmd++
	return []byte(fmt.Sprintf("cmd-%d", h.nextCmd))
}

// one submits cmd to whichever reachable node accepts it as leader and waits
// until at least expected nodes have applied it. With retry it re-submits if
// the first leader loses the entry. It returns the entry's index.
func (h *harness) one(cmd []byte, expected int, retry bool) int {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		h.checkInvariants()
		index := 0
		h.mu.Lock()
		start := h.lastLeader
		h.mu.Unlock()
		for k := 0; k < h.n && index == 0; k++ {
			id := (start+k)%h.n + 1
			if !h.isReachable(id) {
				continue
			}
			if node := h.node(id); node != nil {
				if idx, _, ok := node.Propose(cmd); ok {
					index = idx
					h.mu.Lock()
					h.lastLeader = id - 1
					h.mu.Unlock()
				}
			}
		}
		if index == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		wait := time.Now().Add(2 * time.Second)
		for time.Now().Before(wait) {
			if n, got := h.nCommitted(index); n >= expected && bytes.Equal(got, cmd) {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			h.t.Fatalf("one(%q): not applied by %d nodes", cmd, expected)
		}
		h.mu.Lock()
		h.lastLeader++
		h.mu.Unlock()
	}
	h.t.Fatalf("one(%q): no agreement within 10s", cmd)
	return 0
}

// checkOneLeader waits for a leader among reachable nodes and returns it.
func (h *harness) checkOneLeader() int {
	h.t.Helper()
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		leaders := map[uint64][]int{}
		var top uint64
		for id := 1; id <= h.n; id++ {
			node := h.node(id)
			if node == nil || !h.isReachable(id) {
				continue
			}
			if st := node.Status(); st.Role == "leader" {
				leaders[st.Term] = append(leaders[st.Term], id)
				top = max(top, st.Term)
			}
		}
		for term, ids := range leaders {
			if len(ids) > 1 {
				h.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
		}
		if len(leaders) > 0 {
			return leaders[top][0]
		}
	}
	h.t.Fatal("no leader elected")
	return 0
}

func (h *harness) checkNoLeader() {
	h.t.Helper()
	for id := 1; id <= h.n; id++ {
		if node := h.node(id); node != nil && h.isReachable(id) && node.Status().Role == "leader" {
			h.t.Fatalf("node %d is leader but should not be", id)
		}
	}
}

// checkTerms returns the term all reachable nodes agree on.
func (h *harness) checkTerms() uint64 {
	h.t.Helper()
	var term uint64
	for id := 1; id <= h.n; id++ {
		node := h.node(id)
		if node == nil || !h.isReachable(id) {
			continue
		}
		st := node.Status()
		if term == 0 {
			term = st.Term
		} else if st.Term != term {
			h.t.Fatalf("nodes disagree on the term: %d vs %d", term, st.Term)
		}
	}
	return term
}

func (h *harness) leaderTerms() int {
	h.leaderMu.Lock()
	defer h.leaderMu.Unlock()
	return len(h.leaders)
}
