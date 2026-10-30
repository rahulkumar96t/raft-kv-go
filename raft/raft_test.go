package raft_test

import (
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/transport/simnet"
)

var unreliable = simnet.Options{
	DropRate:     0.1,
	MaxDelay:     20 * time.Millisecond,
	ReorderRate:  0.1,
	ReorderDelay: 150 * time.Millisecond,
}

func TestInitialElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.checkOneLeader()
	term1 := h.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d after an election", term1)
	}
	// With no failures the leader's heartbeats must prevent new elections.
	time.Sleep(2 * electionTimeoutMax)
	if term2 := h.checkTerms(); term2 != term1 {
		t.Errorf("term changed from %d to %d without any failure", term1, term2)
	}
	h.checkOneLeader()
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3, 0)
	leader1 := h.checkOneLeader()
	oldTerm := h.node(leader1).Status().Term

	h.disconnect(leader1)
	leader2 := h.checkOneLeader()
	if leader2 == leader1 {
		t.Fatal("disconnected leader is still the only leader")
	}

	// The old leader rejoins and must step down once it hears the newer term.
	h.connect(leader1)
	deadline := time.Now().Add(3 * time.Second)
	for st := h.node(leader1).Status(); st.Role == "leader" && st.Term == oldTerm; st = h.node(leader1).Status() {
		if time.Now().After(deadline) {
			t.Fatal("old leader never stepped down after rejoining")
		}
		time.Sleep(10 * time.Millisecond)
	}
	leader3 := h.checkOneLeader()

	// Without a quorum nobody may become leader.
	other := leader3%3 + 1
	h.disconnect(leader3)
	h.disconnect(other)
	time.Sleep(2 * electionTimeoutMax)
	h.checkNoLeader()

	h.connect(other)
	h.checkOneLeader()
	h.connect(leader3)
	h.checkOneLeader()
}

// Repeatedly cut a random minority off a 7-node cluster on a lossy network;
// the OnLeader hook fails the test if any term ever has two leaders.
func TestElectionSafetyUnderChurn(t *testing.T) {
	h := newHarness(t, 7, 0)
	// With 3 of 7 nodes cut off, a candidate needs every remaining vote, so
	// keep losses mild here; heavy loss is covered by the agreement tests.
	h.net.SetOptions(simnet.Options{DropRate: 0.03, MaxDelay: 20 * time.Millisecond, ReorderRate: 0.05, ReorderDelay: 100 * time.Millisecond})
	for round := 0; round < 8; round++ {
		down := h.rng.Perm(7)[:3]
		for _, i := range down {
			h.disconnect(i + 1)
		}
		h.checkOneLeader()
		for _, i := range down {
			h.connect(i + 1)
		}
	}
	h.checkOneLeader()
	h.checkInvariants()
	if h.leaderTerms() < 2 {
		t.Fatalf("expected several elections, saw %d", h.leaderTerms())
	}
}

func TestBasicAgreement(t *testing.T) {
	h := newHarness(t, 3, 0)
	prev := 0
	for i := 0; i < 5; i++ {
		index := h.one(h.newCmd(), 3, false)
		if index <= prev {
			t.Fatalf("index %d did not advance past %d", index, prev)
		}
		prev = index
	}
}

func TestFollowerFailure(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one(h.newCmd(), 3, false)

	leader := h.checkOneLeader()
	follower1 := leader%3 + 1
	h.disconnect(follower1)
	h.one(h.newCmd(), 2, false)
	time.Sleep(electionTimeoutMax)
	h.one(h.newCmd(), 2, false)

	// Leader alone: it may accept an entry but must not commit it.
	leader = h.checkOneLeader()
	follower2 := 6 - leader - follower1
	h.disconnect(follower2)
	index, _, ok := h.node(leader).Propose(h.newCmd())
	if !ok {
		t.Fatal("leader rejected the proposal")
	}
	time.Sleep(2 * electionTimeoutMax)
	if n, _ := h.nCommitted(index); n > 0 {
		t.Fatalf("index %d committed without a majority", index)
	}

	h.connect(follower1)
	h.connect(follower2)
	h.one(h.newCmd(), 3, true)
}

// A partitioned old leader keeps accepting entries it can never commit; the
// majority side moves on; after healing, the minority's entries are discarded
// and every log converges (exercising the fast backtracking path).
func TestPartitionAndHeal(t *testing.T) {
	h := newHarness(t, 5, 0)
	h.one(h.newCmd(), 5, false)

	oldLeader := h.checkOneLeader()
	minority := []int{oldLeader, oldLeader%5 + 1}
	var majority []int
	for id := 1; id <= 5; id++ {
		if id != minority[0] && id != minority[1] {
			majority = append(majority, id)
		}
	}
	h.net.Partition(minority, majority)

	var stranded [][]byte
	for i := 0; i < 40; i++ {
		cmd := h.newCmd()
		if _, _, ok := h.node(oldLeader).Propose(cmd); !ok {
			t.Fatal("partitioned leader stopped accepting proposals")
		}
		stranded = append(stranded, cmd)
	}

	// Keep the test client on the majority side.
	for _, id := range minority {
		h.mu.Lock()
		h.reachable[id] = false
		h.mu.Unlock()
	}
	for i := 0; i < 40; i++ {
		h.one(h.newCmd(), 3, true)
	}
	for _, cmd := range stranded {
		if h.wasCommitted(cmd) {
			t.Fatalf("minority entry %q was committed", cmd)
		}
	}

	h.net.Heal()
	for _, id := range minority {
		h.connect(id)
	}
	final := h.one(h.newCmd(), 5, true)
	for _, cmd := range stranded {
		if h.wasCommitted(cmd) {
			t.Fatalf("minority entry %q was applied after healing", cmd)
		}
	}
	for id := 1; id <= 5; id++ {
		if st := h.node(id).Status(); st.CommitIndex < final {
			t.Errorf("node %d commit index %d < %d", id, st.CommitIndex, final)
		}
	}
}

func TestPersistence(t *testing.T) {
	h := newHarness(t, 3, 0)
	h.one(h.newCmd(), 3, false)
	termBefore := h.checkTerms()

	for id := 1; id <= 3; id++ {
		h.crash(id)
	}
	for id := 1; id <= 3; id++ {
		h.start(id)
	}
	h.one(h.newCmd(), 3, true)
	if term := h.node(1).Status().Term; term < termBefore {
		t.Fatalf("term went backwards across a restart: %d -> %d", termBefore, term)
	}

	leader := h.checkOneLeader()
	h.crash(leader)
	h.one(h.newCmd(), 2, true)
	h.start(leader)
	h.one(h.newCmd(), 3, true)

	follower := h.checkOneLeader()%3 + 1
	h.crash(follower)
	h.one(h.newCmd(), 2, true)
	h.start(follower)
	h.one(h.newCmd(), 3, true)
}

// Random crashes, restarts and a lossy, reordering network while proposals
// keep flowing. The harness checks Election Safety and State Machine Safety
// throughout; at the end every node must agree on a fresh entry.
func TestAgreementUnderRandomFailures(t *testing.T) {
	for _, tc := range []struct {
		name          string
		snapshotEvery int
	}{
		{"log only", 0},
		{"with snapshots", 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 5, tc.snapshotEvery)
			h.net.SetOptions(unreliable)
			h.one(h.newCmd(), 1, true)

			up := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}
			crashes := 0
			deadline := time.Now().Add(4 * time.Second)
			for time.Now().Before(deadline) {
				leader := 0
				for id := 1; id <= 5; id++ {
					if node := h.node(id); node != nil {
						if _, _, ok := node.Propose(h.newCmd()); ok {
							leader = id
						}
					}
				}
				time.Sleep(time.Duration(10+h.rng.IntN(90)) * time.Millisecond)
				victim := 0
				switch r := h.rng.IntN(10); {
				case r < 2:
					victim = leader // leader failures force re-elections
				case r < 4:
					victim = h.rng.IntN(5) + 1
				}
				if up[victim] {
					h.crash(victim)
					delete(up, victim)
					crashes++
				}
				// Occasionally drop below a majority, but not for long.
				if len(up) < 3 || h.rng.IntN(3) == 0 {
					if id := h.rng.IntN(5) + 1; !up[id] {
						h.start(id)
						up[id] = true
					}
				}
				h.checkInvariants()
			}

			h.net.SetOptions(simnet.Options{})
			for id := 1; id <= 5; id++ {
				if !up[id] {
					h.start(id)
				}
			}
			h.one(h.newCmd(), 5, true)
			n, _ := h.nCommitted(1)
			if n != 5 {
				t.Fatalf("only %d nodes applied index 1", n)
			}
			h.mu.Lock()
			total := len(h.committed) - 1
			h.mu.Unlock()
			t.Logf("%d crashes, %d entries committed, %d leader terms, network %+v", crashes, total, h.leaderTerms(), h.net.Stats())
			if total < 5 {
				t.Fatalf("only %d entries committed; the cluster barely made progress", total)
			}
			if h.leaderTerms() < 2 {
				t.Fatalf("no re-election happened (%d leader terms)", h.leaderTerms())
			}
		})
	}
}

// A follower that misses so much that the leader has compacted the entries it
// needs must be brought up to date with InstallSnapshot.
func TestSnapshotCatchUp(t *testing.T) {
	h := newHarness(t, 3, 10)
	h.one(h.newCmd(), 3, false)

	leader := h.checkOneLeader()
	lagging := leader%3 + 1
	h.disconnect(lagging)
	for i := 0; i < 60; i++ {
		h.one(h.newCmd(), 2, true)
	}
	if st := h.node(leader).Status(); st.FirstIndex < 10 {
		t.Fatalf("leader did not compact its log: first index %d", st.FirstIndex)
	}

	h.connect(lagging)
	final := h.one(h.newCmd(), 3, true)
	h.mu.Lock()
	installed := h.snapshots[lagging]
	h.mu.Unlock()
	if installed == 0 {
		t.Fatal("lagging follower caught up without receiving a snapshot")
	}

	// Restart everyone: each node must rebuild from its snapshot plus log.
	for id := 1; id <= 3; id++ {
		h.crash(id)
	}
	for id := 1; id <= 3; id++ {
		h.start(id)
	}
	next := h.one(h.newCmd(), 3, true)
	if next <= final {
		t.Fatalf("index went backwards after restart: %d <= %d", next, final)
	}
	for id := 1; id <= 3; id++ {
		h.mu.Lock()
		got := h.snapshots[id]
		h.mu.Unlock()
		if got == 0 {
			t.Errorf("node %d did not restore from its snapshot on restart", id)
		}
	}
}
