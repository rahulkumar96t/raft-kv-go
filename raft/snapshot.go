package raft

import "context"

// Snapshot tells Raft that the service has captured its state up to and
// including index, so the log prefix can be discarded (§7). The service
// calls it from its apply loop with an index it has already applied.
func (n *Node) Snapshot(index int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || index <= n.firstIndex() || index > n.commitIndex {
		return
	}
	n.compactTo(index, n.termAt(index))
	n.snapshot = data
	n.persist()
	n.logger.Debug("compacted log", "through", index, "state_bytes", n.stateSize)
}

func (n *Node) sendSnapshot(peer int, args *InstallSnapshotArgs) bool {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	reply, err := n.transport.InstallSnapshot(ctx, peer, args)
	cancel()
	if err != nil {
		return false
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
	n.matchIndex[peer] = max(n.matchIndex[peer], args.LastIncludedIndex)
	n.nextIndex[peer] = max(n.nextIndex[peer], args.LastIncludedIndex+1)
	n.advanceCommit()
	return n.nextIndex[peer] <= n.lastIndex()
}

// HandleInstallSnapshot implements RPCHandler (§7).
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm || n.role != Follower {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	if args.LastIncludedIndex <= n.commitIndex {
		return reply, nil // we already have everything it covers
	}
	n.compactTo(args.LastIncludedIndex, args.LastIncludedTerm)
	n.snapshot = args.Data
	n.commitIndex = args.LastIncludedIndex
	n.persist()
	n.applyCond.Broadcast() // the applier delivers the snapshot to the service
	return reply, nil
}
