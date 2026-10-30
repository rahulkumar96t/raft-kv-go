package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/rahulkumar96t/raft-kv-go/raft"
)

var (
	// ErrStopped is returned once the server has been stopped.
	ErrStopped = errors.New("kv: server stopped")
	// ErrUnknownOutcome means leadership changed after the request entered
	// the log; it may or may not have been applied. Retrying with the same
	// ClientID/Seq is safe.
	ErrUnknownOutcome = errors.New("kv: leadership changed, outcome unknown")
)

// NotLeaderError is returned when the server's Raft node is not the leader.
// Leader is the ID of the last leader the node heard from (0 if unknown).
type NotLeaderError struct{ Leader int }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "kv: not the leader; leader unknown"
	}
	return fmt.Sprintf("kv: not the leader; try node %d", e.Leader)
}

// Config configures a Server.
type Config struct {
	Raft raft.Config
	// SnapshotThreshold is the persisted Raft state size, in bytes, at which
	// the server snapshots and lets Raft discard the log prefix. 0 disables.
	SnapshotThreshold int
}

// Server is one replica of the store: a Raft node plus the state machine fed
// by its apply channel.
type Server struct {
	node      *raft.Node
	applyCh   chan raft.ApplyMsg
	threshold int
	quit      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once

	mu          sync.Mutex // order: s.mu before the node's internal lock
	sm          *StateMachine
	lastApplied int
	waiters     map[int][]waiter
	stopped     bool
}

type waiter struct {
	term uint64
	ch   chan outcome
}

type outcome struct {
	res Result
	err error
}

// NewServer starts a replica.
func NewServer(cfg Config, transport raft.Transport, persister raft.Persister) (*Server, error) {
	s := &Server{
		applyCh:   make(chan raft.ApplyMsg),
		threshold: cfg.SnapshotThreshold,
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
		sm:        NewStateMachine(),
		waiters:   map[int][]waiter{},
	}
	node, err := raft.New(cfg.Raft, transport, persister, s.applyCh)
	if err != nil {
		return nil, err
	}
	s.node = node
	go s.run()
	return s, nil
}

// Node exposes the underlying Raft node (to register it with a transport).
func (s *Server) Node() *raft.Node { return s.node }

// Stop stops the replica. Pending requests fail with ErrStopped.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.node.Stop()
		close(s.quit)
		<-s.done
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stopped = true
		for _, ws := range s.waiters {
			for _, w := range ws {
				w.ch <- outcome{err: ErrStopped}
			}
		}
		s.waiters = nil
	})
}

// Submit runs op through the Raft log and returns its result once applied.
// It fails fast with *NotLeaderError on followers.
func (s *Server) Submit(ctx context.Context, op Op) (Result, error) {
	if err := op.validate(); err != nil {
		return Result{}, err
	}
	cmd, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	ch := make(chan outcome, 1)

	// Holding s.mu across Propose guarantees the waiter is registered before
	// the apply loop can see the entry.
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return Result{}, ErrStopped
	}
	index, term, isLeader := s.node.Propose(cmd)
	if !isLeader {
		leader := s.node.Status().Leader
		s.mu.Unlock()
		return Result{}, &NotLeaderError{Leader: leader}
	}
	s.waiters[index] = append(s.waiters[index], waiter{term: term, ch: ch})
	s.mu.Unlock()

	select {
	case o := <-ch:
		return o.res, o.err
	case <-ctx.Done():
		s.dropWaiter(index, ch)
		return Result{}, ctx.Err()
	}
}

// LocalRead reads this replica's copy without going through Raft. The value
// may be stale; it exists for tests and debugging.
func (s *Server) LocalRead(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sm.Read(key)
}

func (s *Server) dropWaiter(index int, ch chan outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := s.waiters[index]
	for i, w := range ws {
		if w.ch == ch {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(s.waiters, index)
	} else {
		s.waiters[index] = ws
	}
}

func (s *Server) run() {
	defer close(s.done)
	for {
		select {
		case <-s.quit:
			return
		case msg := <-s.applyCh:
			s.apply(msg)
		}
	}
}

func (s *Server) apply(msg raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.SnapshotValid {
		if msg.SnapshotIndex <= s.lastApplied {
			return
		}
		if err := s.sm.Restore(msg.Snapshot); err != nil {
			panic(fmt.Sprintf("kv: restore snapshot at %d: %v", msg.SnapshotIndex, err))
		}
		s.lastApplied = msg.SnapshotIndex
		// Requests waiting inside the snapshot's range cannot tell whether
		// their entry made it; let the clients retry.
		for index, ws := range s.waiters {
			if index <= msg.SnapshotIndex {
				for _, w := range ws {
					w.ch <- outcome{err: ErrUnknownOutcome}
				}
				delete(s.waiters, index)
			}
		}
		return
	}
	if !msg.CommandValid || msg.CommandIndex <= s.lastApplied {
		return
	}
	s.lastApplied = msg.CommandIndex

	var res Result
	if len(msg.Command) > 0 { // empty = a leader's no-op
		var op Op
		if err := json.Unmarshal(msg.Command, &op); err != nil {
			panic(fmt.Sprintf("kv: undecodable log entry %d: %v", msg.CommandIndex, err))
		}
		res, _ = s.sm.Apply(op)
	}
	for _, w := range s.waiters[msg.CommandIndex] {
		if w.term == msg.CommandTerm {
			// Same index and term means it is the entry we proposed.
			w.ch <- outcome{res: res}
		} else {
			w.ch <- outcome{err: ErrUnknownOutcome}
		}
	}
	delete(s.waiters, msg.CommandIndex)

	if s.threshold > 0 && s.node.StateSize() >= s.threshold {
		snap, err := s.sm.Snapshot()
		if err != nil {
			panic(fmt.Sprintf("kv: snapshot: %v", err))
		}
		s.node.Snapshot(msg.CommandIndex, snap)
	}
}
