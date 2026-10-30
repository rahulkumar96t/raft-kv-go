package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// White-box tests of the RPC rules on nodes whose goroutines are not running,
// so nothing happens unless the test calls it.

type nopTransport struct{}

var errUnreachable = errors.New("unreachable")

func (nopTransport) RequestVote(context.Context, int, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errUnreachable
}
func (nopTransport) AppendEntries(context.Context, int, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, errUnreachable
}
func (nopTransport) InstallSnapshot(context.Context, int, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, errUnreachable
}

func idleNode(t *testing.T, p Persister) *Node {
	t.Helper()
	n, err := newNode(Config{
		ID:                 1,
		Peers:              []int{1, 2, 3},
		ElectionTimeoutMin: time.Hour,
		ElectionTimeoutMax: time.Hour,
	}, nopTransport{}, p, make(chan ApplyMsg, 64))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// withLog sets the node's log to entries 1..len(terms) with the given terms.
func withLog(n *Node, terms ...uint64) {
	n.entries = []LogEntry{{}}
	for i, term := range terms {
		n.entries = append(n.entries, LogEntry{Index: i + 1, Term: term, Command: []byte{byte(i + 1)}})
	}
	if len(terms) > 0 {
		n.currentTerm = max(n.currentTerm, terms[len(terms)-1])
	}
}

func logTerms(n *Node) []uint64 {
	var out []uint64
	for _, e := range n.entries[1:] {
		out = append(out, e.Term)
	}
	return out
}

func equalTerms(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	n.currentTerm = 5
	reply, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 4, CandidateID: 2})
	if reply.VoteGranted || reply.Term != 5 {
		t.Fatalf("got %+v, want rejection with term 5", reply)
	}
}

func TestRequestVoteOneVotePerTerm(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	if r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("first candidate should get the vote")
	}
	if r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: 3}); r.VoteGranted {
		t.Fatal("second candidate in the same term must not get a vote")
	}
	if r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("a repeated request from the same candidate should be granted again")
	}
	if r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 2, CandidateID: 3}); !r.VoteGranted {
		t.Fatal("a new term allows a new vote")
	}
}

func TestRequestVoteElectionRestriction(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 2) // last entry: index 3, term 2
	cases := []struct {
		name      string
		lastIndex int
		lastTerm  uint64
		grant     bool
	}{
		{"older last term", 10, 1, false},
		{"same term, shorter log", 2, 2, false},
		{"same term, same length", 3, 2, true},
		{"newer last term, shorter log", 1, 3, true},
	}
	for i, c := range cases {
		term := uint64(10 + i) // fresh term each time, so votedFor is clear
		r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: term, CandidateID: 2, LastLogIndex: c.lastIndex, LastLogTerm: c.lastTerm})
		if r.VoteGranted != c.grant {
			t.Errorf("%s: granted=%v, want %v", c.name, r.VoteGranted, c.grant)
		}
	}
}

func TestVoteAndTermSurviveRestart(t *testing.T) {
	p := NewMemoryPersister()
	n := idleNode(t, p)
	withLog(n, 1, 1)
	n.persist()
	if r, _ := n.HandleRequestVote(&RequestVoteArgs{Term: 7, CandidateID: 3, LastLogIndex: 2, LastLogTerm: 1}); !r.VoteGranted {
		t.Fatal("vote should be granted")
	}
	restarted := idleNode(t, p.Copy())
	if restarted.currentTerm != 7 || restarted.votedFor != 3 {
		t.Fatalf("restored term=%d votedFor=%d, want 7 and 3", restarted.currentTerm, restarted.votedFor)
	}
	if !equalTerms(logTerms(restarted), []uint64{1, 1}) {
		t.Fatalf("restored log terms %v", logTerms(restarted))
	}
	if r, _ := restarted.HandleRequestVote(&RequestVoteArgs{Term: 7, CandidateID: 2, LastLogIndex: 9, LastLogTerm: 1}); r.VoteGranted {
		t.Fatal("restarted node voted twice in term 7")
	}
}

func TestAppendEntriesRejectsStaleLeader(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	n.currentTerm = 3
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: 2})
	if r.Success || r.Term != 3 {
		t.Fatalf("got %+v", r)
	}
}

func TestAppendEntriesMissingPrevEntry(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1)
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 1})
	if r.Success || r.ConflictIndex != 3 || r.ConflictTerm != 0 {
		t.Fatalf("got %+v, want failure with ConflictIndex 3", r)
	}
}

func TestAppendEntriesConflictHintNamesWholeTerm(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 2, 2, 2)
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 3})
	if r.Success || r.ConflictTerm != 2 || r.ConflictIndex != 3 {
		t.Fatalf("got %+v, want ConflictTerm 2 starting at index 3", r)
	}
}

func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 2, 2)
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 3, Term: 3}},
	})
	if !r.Success {
		t.Fatalf("append failed: %+v", r)
	}
	if got := logTerms(n); !equalTerms(got, []uint64{1, 1, 3}) {
		t.Fatalf("log terms %v, want [1 1 3]", got)
	}
}

func TestAppendEntriesDelayedShortRequestKeepsLaterEntries(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 1, 1)
	// An old AppendEntries carrying only entry 2 arrives late; entries 3-4
	// were accepted since and must survive.
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 2, Term: 1}},
	})
	if !r.Success || len(n.entries)-1 != 4 {
		t.Fatalf("success=%v, log length %d, want true and 4", r.Success, len(n.entries)-1)
	}
}

func TestAppendEntriesCommitIndexLimitedToVerifiedPrefix(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 1, 1, 1)
	// The leader has committed index 5 but this RPC only proves agreement up
	// to index 2: entries 3-5 here might be stale ones from another leader.
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 5})
	if !r.Success || n.commitIndex != 2 {
		t.Fatalf("success=%v commitIndex=%d, want true and 2", r.Success, n.commitIndex)
	}
}

func TestLeaderBacktrackUsesConflictHint(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6) // leader log, indexes 1..10
	n.currentTerm = 6
	args := &AppendEntriesArgs{PrevLogIndex: 10}
	cases := []struct {
		name  string
		reply AppendEntriesReply
		want  int
	}{
		// Follower has term 4 at index 10: leader has term 4, retry after its last one (index 5).
		{"leader has the term", AppendEntriesReply{ConflictTerm: 4, ConflictIndex: 6}, 6},
		// Follower has term 2 from index 4: leader has no term 2, skip to 4.
		{"leader lacks the term", AppendEntriesReply{ConflictTerm: 2, ConflictIndex: 4}, 4},
		// Follower's log is too short: it ends at index 6.
		{"follower log too short", AppendEntriesReply{ConflictIndex: 7}, 7},
		// Hints never move nextIndex forward.
		{"bogus hint", AppendEntriesReply{ConflictIndex: 50}, 10},
	}
	for _, c := range cases {
		if got := n.backtrack(args, &c.reply); got != c.want {
			t.Errorf("%s: nextIndex %d, want %d", c.name, got, c.want)
		}
	}
}

// Figure 8 of the paper: an entry from an earlier term that sits on a
// majority must not be committed by counting replicas.
func TestCommitRequiresEntryFromCurrentTerm(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 2)
	n.currentTerm = 4
	n.role = Leader
	n.matchIndex[2], n.matchIndex[3] = 2, 0
	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed index %d from an old term by counting replicas", n.commitIndex)
	}
	n.appendLocal([]byte("x")) // index 3, term 4
	n.matchIndex[2] = 3
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("commitIndex %d, want 3 once a term-4 entry is on a majority", n.commitIndex)
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	p := NewMemoryPersister()
	n := idleNode(t, p)
	withLog(n, 1, 1, 2, 2, 3)
	n.commitIndex = 4
	n.persist()
	before := n.stateSize
	n.Snapshot(3, []byte("state@3"))
	if n.firstIndex() != 3 || n.termAt(3) != 2 || n.lastIndex() != 5 {
		t.Fatalf("after compaction first=%d last=%d", n.firstIndex(), n.lastIndex())
	}
	if n.stateSize >= before {
		t.Fatalf("persisted state did not shrink: %d -> %d", before, n.stateSize)
	}
	n.Snapshot(5, []byte("too far")) // beyond commitIndex: ignored
	if n.firstIndex() != 3 {
		t.Fatal("snapshot beyond the commit index must be ignored")
	}

	restarted := idleNode(t, p.Copy())
	if restarted.firstIndex() != 3 || string(restarted.snapshot) != "state@3" || restarted.commitIndex != 3 {
		t.Fatalf("restart: first=%d snapshot=%q commit=%d", restarted.firstIndex(), restarted.snapshot, restarted.commitIndex)
	}
}

func TestInstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 2, 2, 2)
	r, _ := n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 2, LeaderID: 2, LastIncludedIndex: 3, LastIncludedTerm: 2, Data: []byte("s")})
	if r.Term != 2 {
		t.Fatalf("reply term %d", r.Term)
	}
	if n.firstIndex() != 3 || n.lastIndex() != 5 || n.commitIndex != 3 {
		t.Fatalf("first=%d last=%d commit=%d, want 3, 5, 3", n.firstIndex(), n.lastIndex(), n.commitIndex)
	}
}

func TestInstallSnapshotDiscardsConflictingLog(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 1, 1)
	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 3, LeaderID: 2, LastIncludedIndex: 3, LastIncludedTerm: 3, Data: []byte("s")})
	if n.firstIndex() != 3 || n.lastIndex() != 3 {
		t.Fatalf("first=%d last=%d, want the log reduced to the snapshot", n.firstIndex(), n.lastIndex())
	}
}

func TestAppendEntriesAfterSnapshotSkipsCompactedPrefix(t *testing.T) {
	n := idleNode(t, NewMemoryPersister())
	withLog(n, 1, 1, 1, 1)
	n.commitIndex = 4
	n.Snapshot(4, []byte("s"))
	// A leader that thinks we are further behind resends entries 2..5.
	r, _ := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}, {Index: 5, Term: 1}},
	})
	if !r.Success || n.lastIndex() != 5 || n.firstIndex() != 4 {
		t.Fatalf("success=%v first=%d last=%d", r.Success, n.firstIndex(), n.lastIndex())
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{ID: 0, Peers: []int{0}},
		{ID: 1, Peers: []int{2, 3}},
		{ID: 1, Peers: []int{1, 1}},
		{ID: 1, Peers: []int{1}, ElectionTimeoutMin: time.Second, ElectionTimeoutMax: time.Millisecond},
		{ID: 1, Peers: []int{1}, HeartbeatInterval: time.Second, ElectionTimeoutMin: time.Millisecond, ElectionTimeoutMax: time.Second},
	}
	for i, c := range bad {
		if _, err := newNode(c, nopTransport{}, NewMemoryPersister(), nil); err == nil {
			t.Errorf("config %d: expected an error", i)
		}
	}
}

func TestSingleNodeClusterCommitsAlone(t *testing.T) {
	applyCh := make(chan ApplyMsg, 8)
	n, err := New(Config{ID: 1, Peers: []int{1}, ElectionTimeoutMin: 20 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond}, nopTransport{}, NewMemoryPersister(), applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, _, ok := n.Propose([]byte("hello")); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for {
		select {
		case msg := <-applyCh:
			if string(msg.Command) == "hello" {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("proposal was not applied")
		}
	}
}
