package raft

import "context"

// Role is the role a node currently plays in the cluster.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// LogEntry is one slot of the replicated log. Command is opaque to Raft. A
// nil Command marks the no-op entry a new leader appends at the start of its
// term so that entries from earlier terms can be committed.
type LogEntry struct {
	Index   int    `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// RequestVoteArgs is sent by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  int    `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply answers RequestVoteArgs.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesArgs replicates log entries and doubles as the heartbeat (§5.3).
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesReply answers AppendEntriesArgs. On a consistency-check
// failure the follower fills in ConflictTerm/ConflictIndex so the leader can
// skip a whole term per round trip instead of one entry.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex int    `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// InstallSnapshotArgs ships the leader's snapshot to a follower whose next
// needed entry has already been compacted away (§7). The snapshot is sent in
// one piece rather than in chunks.
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          int    `json:"leader_id"`
	LastIncludedIndex int    `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply answers InstallSnapshotArgs.
type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// Transport delivers RPCs to other cluster members. Raft always passes a
// context with a deadline; implementations must give up when it expires.
type Transport interface {
	RequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler is the receiving side of a Transport. *Node implements it.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
	HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// ApplyMsg is delivered on the apply channel, strictly in log order. Either
// CommandValid is set (a committed entry; Command is nil for a leader's no-op)
// or SnapshotValid is set (replace the state machine with Snapshot, which
// covers every entry up to and including SnapshotIndex).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  uint64
}

// Status is a point-in-time view of a node, for diagnostics and tests.
type Status struct {
	ID          int    `json:"id"`
	Term        uint64 `json:"term"`
	Role        string `json:"role"`
	Leader      int    `json:"leader"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
	FirstIndex  int    `json:"first_index"`
	LastIndex   int    `json:"last_index"`
}
