package simnet

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/raft"
)

// recorder is a fake RPC handler that records the terms it receives.
type recorder struct {
	mu    sync.Mutex
	terms []uint64
	calls atomic.Int64
}

func (r *recorder) HandleRequestVote(a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.terms = append(r.terms, a.Term)
	r.mu.Unlock()
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}, nil
}

func (r *recorder) HandleAppendEntries(a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	r.calls.Add(1)
	// Mutate the arguments: with a real wire in between, the sender must not see this.
	if len(a.Entries) > 0 {
		a.Entries[0].Command[0] = 'X'
	}
	return &raft.AppendEntriesReply{Term: a.Term, Success: true}, nil
}

func (r *recorder) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	r.calls.Add(1)
	return &raft.InstallSnapshotReply{Term: a.Term}, nil
}

func vote(net *Network, from, to int, term uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := net.Transport(from).RequestVote(ctx, to, &raft.RequestVoteArgs{Term: term})
	return err
}

func TestReliableDeliveryCopiesMessages(t *testing.T) {
	net := New(Options{})
	r := &recorder{}
	net.Register(1, &recorder{})
	net.Register(2, r)

	reply, err := net.Transport(1).RequestVote(context.Background(), 2, &raft.RequestVoteArgs{Term: 4})
	if err != nil || !reply.VoteGranted || reply.Term != 4 {
		t.Fatalf("reply %+v err %v", reply, err)
	}
	args := &raft.AppendEntriesArgs{Term: 1, Entries: []raft.LogEntry{{Index: 1, Term: 1, Command: []byte("a")}}}
	if _, err := net.Transport(1).AppendEntries(context.Background(), 2, args); err != nil {
		t.Fatal(err)
	}
	if string(args.Entries[0].Command) != "a" {
		t.Fatal("receiver mutated the sender's memory: messages are not copied")
	}
}

func TestDisconnectAndPartition(t *testing.T) {
	net := New(Options{})
	r2, r3 := &recorder{}, &recorder{}
	net.Register(1, &recorder{})
	net.Register(2, r2)
	net.Register(3, r3)

	net.Disconnect(2)
	if err := vote(net, 1, 2, 1, 30*time.Millisecond); err == nil {
		t.Fatal("call to a disconnected node succeeded")
	}
	net.Connect(2)
	if err := vote(net, 1, 2, 1, time.Second); err != nil {
		t.Fatalf("after reconnect: %v", err)
	}

	net.Partition([]int{1, 2}, []int{3})
	if err := vote(net, 1, 2, 1, time.Second); err != nil {
		t.Fatalf("same side of the partition: %v", err)
	}
	if err := vote(net, 1, 3, 1, 30*time.Millisecond); err == nil {
		t.Fatal("call across the partition succeeded")
	}
	net.Heal()
	if err := vote(net, 1, 3, 1, time.Second); err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if got := r3.calls.Load(); got != 1 {
		t.Fatalf("node 3 handled %d calls, want 1 (blocked calls must not be delivered)", got)
	}
	if s := net.Stats(); s.Unreachable != 2 || s.Calls != 5 || s.Delivered != 3 {
		t.Fatalf("stats %+v", s)
	}
}

func TestPartitionIsolatesUnlistedNodes(t *testing.T) {
	net := New(Options{})
	for id := 1; id <= 3; id++ {
		net.Register(id, &recorder{})
	}
	net.Partition([]int{1, 2})
	if err := vote(net, 1, 3, 1, 30*time.Millisecond); err == nil {
		t.Fatal("unlisted node 3 should be isolated")
	}
}

func TestDropEverything(t *testing.T) {
	net := New(Options{DropRate: 1})
	r := &recorder{}
	net.Register(1, &recorder{})
	net.Register(2, r)
	for i := 0; i < 5; i++ {
		if err := vote(net, 1, 2, 1, 10*time.Millisecond); err == nil {
			t.Fatal("call succeeded with DropRate 1")
		}
	}
	if r.calls.Load() != 0 {
		t.Fatal("dropped requests reached the handler")
	}
}

// The same seed must produce the same fault decisions.
func TestSeedDeterminesFaults(t *testing.T) {
	outcomes := func(seed int64) []bool {
		net := New(Options{Seed: seed, DropRate: 0.5})
		net.Register(1, &recorder{})
		net.Register(2, &recorder{})
		var got []bool
		for i := 0; i < 40; i++ {
			got = append(got, vote(net, 1, 2, 1, 20*time.Millisecond) == nil)
		}
		return got
	}
	a, b, c := outcomes(42), outcomes(42), outcomes(43)
	same := func(x, y []bool) bool {
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
	if !same(a, b) {
		t.Fatal("same seed produced different drop patterns")
	}
	if same(a, c) {
		t.Fatal("different seeds produced identical drop patterns")
	}
}

func TestDelaysReorderMessages(t *testing.T) {
	net := New(Options{Seed: 1, MaxDelay: 30 * time.Millisecond})
	r := &recorder{}
	net.Register(1, &recorder{})
	net.Register(2, r)
	var wg sync.WaitGroup
	for term := uint64(1); term <= 30; term++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = vote(net, 1, 2, term, time.Second)
		}()
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.terms) != 30 {
		t.Fatalf("delivered %d of 30", len(r.terms))
	}
	if sort.SliceIsSorted(r.terms, func(i, j int) bool { return r.terms[i] < r.terms[j] }) {
		t.Fatal("random delays never reordered anything")
	}
}

// A request may still arrive after the caller timed out, as on a real network.
func TestLateDeliveryAfterTimeout(t *testing.T) {
	net := New(Options{MinDelay: 50 * time.Millisecond, MaxDelay: 51 * time.Millisecond})
	r := &recorder{}
	net.Register(1, &recorder{})
	net.Register(2, r)
	if err := vote(net, 1, 2, 9, 10*time.Millisecond); err == nil {
		t.Fatal("expected a timeout")
	}
	time.Sleep(100 * time.Millisecond)
	if r.calls.Load() != 1 {
		t.Fatal("the delayed request was never delivered")
	}
}
