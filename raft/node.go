// Package raft implements the Raft consensus algorithm: leader election, log
// replication, persistence and log compaction with snapshots. It is a
// teaching implementation that follows the extended Raft paper (Ongaro &
// Ousterhout, 2014) closely; section numbers in comments refer to that paper.
//
// A Node never talks to the network directly. Outgoing RPCs go through a
// Transport and incoming ones arrive via the RPCHandler methods, so the same
// code runs over an in-memory simulated network in tests and over HTTP in a
// real cluster.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"
	"time"
)

// ErrStopped is returned by the RPC handlers of a stopped node.
var ErrStopped = errors.New("raft: node stopped")

// Node is one Raft peer. All exported methods are safe for concurrent use.
//
// Concurrency model: one mutex guards all state. Long-running work (RPCs,
// delivering to the apply channel) happens outside the lock in dedicated
// goroutines: a ticker for timers, one replicator per peer (so at most one
// AppendEntries/InstallSnapshot is in flight per follower) and one applier.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	id        int
	peers     []int // other members, sorted
	transport Transport
	persister Persister
	applyCh   chan<- ApplyMsg
	logger    *slog.Logger
	rng       *rand.Rand

	// Persistent state, saved before the node answers any RPC that depends on it.
	currentTerm uint64
	votedFor    int        // 0 = none
	entries     []LogEntry // entries[0] is a sentinel: index/term of the last compacted entry
	snapshot    []byte

	// Volatile state.
	role             Role
	leaderID         int
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time
	nextHeartbeat    time.Time
	stateSize        int

	// Leader state, reinitialised on every election win.
	nextIndex  map[int]int
	matchIndex map[int]int
	triggers   map[int]chan struct{}

	applyCond *sync.Cond
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// New restores a node from persister and starts it. Committed entries (and
// snapshots) are delivered on applyCh in log order; the caller must keep
// draining it.
func New(cfg Config, transport Transport, persister Persister, applyCh chan<- ApplyMsg) (*Node, error) {
	n, err := newNode(cfg, transport, persister, applyCh)
	if err != nil {
		return nil, err
	}
	for _, p := range n.peers {
		n.wg.Add(1)
		go n.replicator(p, n.triggers[p])
	}
	n.wg.Add(2)
	go n.ticker()
	go n.applier()
	return n, nil
}

// newNode builds a node without starting its goroutines (used directly by
// white-box tests).
func newNode(cfg Config, transport Transport, persister Persister, applyCh chan<- ApplyMsg) (*Node, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		transport:  transport,
		persister:  persister,
		applyCh:    applyCh,
		logger:     cfg.Logger.With("node", cfg.ID),
		rng:        rand.New(rand.NewPCG(uint64(seed), uint64(cfg.ID))),
		entries:    []LogEntry{{}},
		nextIndex:  map[int]int{},
		matchIndex: map[int]int{},
		triggers:   map[int]chan struct{}{},
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.peers = append(n.peers, p)
			n.triggers[p] = make(chan struct{}, 1)
		}
	}
	sort.Ints(n.peers)
	n.applyCond = sync.NewCond(&n.mu)

	state, snapshot, err := persister.Load()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("raft: load persisted state: %w", err)
	}
	if err := n.restore(state, snapshot); err != nil {
		cancel()
		return nil, fmt.Errorf("raft: decode persisted state: %w", err)
	}
	// Everything covered by the snapshot is committed. lastApplied stays 0 so
	// the applier hands the snapshot to the state machine first.
	n.commitIndex = n.firstIndex()
	n.resetElectionTimer()
	return n, nil
}

// Stop shuts the node down and waits for its goroutines. A stopped node
// rejects RPCs and proposals; restart it by calling New with its persister.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.cancel()
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends command to the log if this node is the leader. It returns
// the index the command will occupy if it commits, the current term, and
// whether this node is the leader. A true result does not mean the command
// will commit: leadership may be lost first.
func (n *Node) Propose(command []byte) (index int, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, n.currentTerm, false
	}
	index = n.appendLocal(command)
	n.triggerAll()
	n.advanceCommit() // a single-node cluster commits immediately
	return index, n.currentTerm, true
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role.String(),
		Leader:      n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		FirstIndex:  n.firstIndex(),
		LastIndex:   n.lastIndex(),
	}
}

// StateSize is the size in bytes of the last persisted Raft state (excluding
// the snapshot). Services use it to decide when to snapshot.
func (n *Node) StateSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stateSize
}

// --- log helpers (callers hold mu) ---

func (n *Node) firstIndex() int  { return n.entries[0].Index }
func (n *Node) lastIndex() int   { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64 { return n.entries[len(n.entries)-1].Term }
func (n *Node) quorum() int      { return (len(n.peers)+1)/2 + 1 }

func (n *Node) termAt(index int) uint64 {
	off := index - n.firstIndex()
	if off < 0 || off >= len(n.entries) {
		panic(fmt.Sprintf("raft: node %d: index %d outside log [%d, %d]", n.id, index, n.firstIndex(), n.lastIndex()))
	}
	return n.entries[off].Term
}

func (n *Node) appendLocal(command []byte) int {
	index := n.lastIndex() + 1
	n.entries = append(n.entries, LogEntry{Index: index, Term: n.currentTerm, Command: command})
	n.persist()
	return index
}

// compactTo makes (index, term) the new sentinel, keeping later entries only
// if the log really contains that entry (Log Matching then guarantees the
// suffix is consistent with whoever produced the snapshot).
func (n *Node) compactTo(index int, term uint64) {
	kept := []LogEntry{{Index: index, Term: term}}
	if index >= n.firstIndex() && index < n.lastIndex() && n.termAt(index) == term {
		kept = append(kept, n.entries[index-n.firstIndex()+1:]...)
	}
	n.entries = kept
}

// --- persistence ---

type persistentState struct {
	Term     uint64
	VotedFor int
	Log      []LogEntry
}

// persist writes term, vote and log (plus the current snapshot). A node that
// cannot make its state durable must not keep voting or acknowledging
// entries, so a write failure stops the process (fail-stop).
func (n *Node) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{Term: n.currentTerm, VotedFor: n.votedFor, Log: n.entries}); err != nil {
		panic(fmt.Sprintf("raft: node %d: encode state: %v", n.id, err))
	}
	if err := n.persister.Save(buf.Bytes(), n.snapshot); err != nil {
		panic(fmt.Sprintf("raft: node %d: persist state: %v", n.id, err))
	}
	n.stateSize = buf.Len()
}

func (n *Node) restore(state, snapshot []byte) error {
	if len(state) == 0 {
		return nil
	}
	var ps persistentState
	if err := gob.NewDecoder(bytes.NewReader(state)).Decode(&ps); err != nil {
		return err
	}
	if len(ps.Log) == 0 {
		return errors.New("persisted log is missing its sentinel entry")
	}
	n.currentTerm, n.votedFor, n.entries, n.snapshot = ps.Term, ps.VotedFor, ps.Log, snapshot
	n.stateSize = len(state)
	return nil
}

// --- timers ---

func (n *Node) resetElectionTimer() {
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	n.electionDeadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int64N(span+1)))
}

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-t.C:
			n.tick(now)
		}
	}
}

func (n *Node) tick(now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if n.role == Leader {
		if !now.Before(n.nextHeartbeat) {
			n.nextHeartbeat = now.Add(n.cfg.HeartbeatInterval)
			n.triggerAll()
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElection()
	}
}

// --- applying committed entries ---

// applier is the only goroutine that sends on applyCh, which keeps snapshots
// and entries in log order.
func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.firstIndex() <= n.lastApplied && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		if n.firstIndex() > n.lastApplied {
			// The state machine is behind the snapshot (start-up, or one was
			// installed by the leader): hand it over before any later entry.
			msg := ApplyMsg{SnapshotValid: true, Snapshot: n.snapshot, SnapshotIndex: n.firstIndex(), SnapshotTerm: n.entries[0].Term}
			n.mu.Unlock()
			ok := n.deliver(msg)
			n.mu.Lock()
			if !ok {
				return
			}
			n.lastApplied = max(n.lastApplied, msg.SnapshotIndex)
			continue
		}
		first := n.firstIndex()
		batch := slices.Clone(n.entries[n.lastApplied+1-first : n.commitIndex+1-first])
		n.mu.Unlock()
		for _, e := range batch {
			if !n.deliver(ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term}) {
				n.mu.Lock()
				return
			}
		}
		n.mu.Lock()
		n.lastApplied = max(n.lastApplied, batch[len(batch)-1].Index)
	}
}

func (n *Node) deliver(msg ApplyMsg) bool {
	select {
	case n.applyCh <- msg:
		return true
	case <-n.ctx.Done():
		return false
	}
}
