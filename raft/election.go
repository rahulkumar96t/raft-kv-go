package raft

import (
	"context"
	"time"
)

// startElection turns the node into a candidate for the next term (§5.2).
func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = 0
	n.persist()
	n.resetElectionTimer()
	n.logger.Debug("starting election", "term", n.currentTerm)

	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	votes := 1 // our own; guarded by mu like everything else
	if votes >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		n.wg.Add(1)
		go n.solicitVote(p, args, &votes)
	}
}

func (n *Node) solicitVote(peer int, args RequestVoteArgs, votes *int) {
	defer n.wg.Done()
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	reply, err := n.transport.RequestVote(ctx, peer, &args)
	cancel()
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	// Ignore votes for an election we are no longer running.
	if n.role != Candidate || n.currentTerm != args.Term || !reply.VoteGranted {
		return
	}
	*votes++
	if *votes >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// A leader may only commit entries from its own term by counting replicas
	// (§5.4.2). Appending a no-op right away lets entries from earlier terms
	// commit without waiting for the next client request (§8).
	n.appendLocal(nil)
	n.logger.Info("became leader", "term", n.currentTerm, "last_index", n.lastIndex())
	if n.cfg.OnLeader != nil {
		n.cfg.OnLeader(n.id, n.currentTerm)
	}
	n.nextHeartbeat = time.Now().Add(n.cfg.HeartbeatInterval)
	n.triggerAll()
	n.advanceCommit()
}

// stepDown adopts a newer term (clearing the vote) and/or returns to follower.
func (n *Node) stepDown(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
		n.leaderID = 0
		n.persist()
	}
	if n.role != Follower {
		if n.role == Leader {
			n.logger.Info("stepping down", "term", n.currentTerm)
			// Do not start an election immediately just because the old
			// deadline lapsed while we were leading.
			n.resetElectionTimer()
		}
		n.role = Follower
	}
}

// HandleRequestVote implements RPCHandler (§5.2, §5.4.1).
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	// Election restriction: only vote for candidates whose log is at least as
	// up to date as ours, so every leader holds all committed entries.
	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())
	if (n.votedFor == 0 || n.votedFor == args.CandidateID) && upToDate {
		if n.votedFor == 0 {
			n.votedFor = args.CandidateID
			n.persist()
		}
		n.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply, nil
}
