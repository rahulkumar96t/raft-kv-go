package raft

import (
	"context"
	"fmt"
)

func (n *Node) triggerAll() {
	for _, ch := range n.triggers {
		select {
		case ch <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
}

// replicator owns the leader's conversation with one follower. Keeping one
// RPC in flight per follower bounds memory and keeps nextIndex bookkeeping
// simple; consecutive proposals are batched into the next AppendEntries.
func (n *Node) replicator(peer int, trigger <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-trigger:
		}
		for n.ctx.Err() == nil && n.replicateOnce(peer) {
		}
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to peer and
// reports whether there is more to send right away.
func (n *Node) replicateOnce(peer int) bool {
	n.mu.Lock()
	if n.stopped || n.role != Leader {
		n.mu.Unlock()
		return false
	}
	if n.nextIndex[peer] <= n.firstIndex() {
		// The entries the follower needs were compacted away.
		args := &InstallSnapshotArgs{
			Term:              n.currentTerm,
			LeaderID:          n.id,
			LastIncludedIndex: n.firstIndex(),
			LastIncludedTerm:  n.entries[0].Term,
			Data:              n.snapshot,
		}
		n.mu.Unlock()
		return n.sendSnapshot(peer, args)
	}
	prev := n.nextIndex[peer] - 1
	last := min(n.lastIndex(), prev+n.cfg.MaxEntriesPerRPC)
	first := n.firstIndex()
	entries := make([]LogEntry, last-prev)
	copy(entries, n.entries[prev+1-first:last+1-first])
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	reply, err := n.transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return false // retried on the next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return false
	}
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return false
	}
	if n.role != Leader || n.currentTerm != args.Term {
		return false
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		n.matchIndex[peer] = max(n.matchIndex[peer], match)
		n.nextIndex[peer] = max(n.nextIndex[peer], match+1)
		n.advanceCommit()
	} else {
		n.nextIndex[peer] = n.backtrack(args, reply)
	}
	return n.nextIndex[peer] <= n.lastIndex()
}

// backtrack picks the next nextIndex after a rejected AppendEntries using the
// follower's conflict hint: if the leader has entries from the follower's
// conflicting term, resume just after its last one; otherwise skip the whole
// term. The result is always below the rejected attempt, so this terminates.
func (n *Node) backtrack(args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := min(args.PrevLogIndex, n.lastIndex()); i > n.firstIndex(); i-- {
			t := n.termAt(i)
			if t == reply.ConflictTerm {
				next = i + 1
				break
			}
			if t < reply.ConflictTerm {
				break
			}
		}
	}
	return max(1, min(next, args.PrevLogIndex))
}

// advanceCommit moves commitIndex to the highest index stored on a majority,
// but only by counting replicas of an entry from the current term (§5.4.2,
// Figure 8). Earlier entries then commit indirectly.
func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	for index := n.lastIndex(); index > n.commitIndex; index-- {
		if n.termAt(index) != n.currentTerm {
			break // terms only decrease further back
		}
		count := 1 // the leader itself
		for _, p := range n.peers {
			if n.matchIndex[p] >= index {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = index
			n.applyCond.Broadcast()
			n.triggerAll() // tell followers promptly
			return
		}
	}
}

// HandleAppendEntries implements RPCHandler (§5.3).
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm || n.role != Follower {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	prev, entries := args.PrevLogIndex, args.Entries
	switch {
	case prev < n.firstIndex():
		// The prefix is already in our snapshot, and committed entries are
		// identical everywhere, so skip the part we compacted.
		skip := n.firstIndex() - prev
		if skip >= len(entries) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev = n.firstIndex()
	case prev > n.lastIndex():
		reply.ConflictIndex = n.lastIndex() + 1
		return reply, nil
	case n.termAt(prev) != args.PrevLogTerm:
		t := n.termAt(prev)
		i := prev
		for i-1 > n.firstIndex() && n.termAt(i-1) == t {
			i--
		}
		reply.ConflictTerm, reply.ConflictIndex = t, i
		return reply, nil
	}

	// Append new entries, truncating only on a real conflict: a delayed,
	// shorter AppendEntries must not erase entries we already accepted.
	for i, e := range entries {
		index := prev + 1 + i
		if e.Index != index {
			return nil, fmt.Errorf("raft: malformed AppendEntries: entry %d has index %d", index, e.Index)
		}
		if index <= n.lastIndex() {
			if n.termAt(index) == e.Term {
				continue
			}
			if index <= n.commitIndex {
				panic(fmt.Sprintf("raft: node %d: asked to overwrite committed entry %d", n.id, index))
			}
			n.entries = n.entries[:index-n.firstIndex()]
		}
		n.entries = append(n.entries, entries[i:]...)
		n.persist()
		break
	}

	if args.LeaderCommit > n.commitIndex {
		// Only trust the leader's commit index up to the last entry this RPC
		// proved we share with it.
		if c := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply, nil
}
