package kv_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/internal/lincheck"
	"github.com/rahulkumar96t/raft-kv-go/kv"
	"github.com/rahulkumar96t/raft-kv-go/transport/simnet"
)

func TestStateMachineDeduplicates(t *testing.T) {
	sm := kv.NewStateMachine()
	sm.Apply(kv.Op{Kind: kv.OpPut, Key: "k", Value: "v", ClientID: "c", Seq: 1})
	del := kv.Op{Kind: kv.OpDelete, Key: "k", ClientID: "c", Seq: 2}
	first, dup1 := sm.Apply(del)
	again, dup2 := sm.Apply(del)
	if !first.Found || dup1 {
		t.Fatalf("first delete: %+v dup=%v", first, dup1)
	}
	if !again.Found || !dup2 {
		t.Fatalf("retried delete must return the cached result: %+v dup=%v", again, dup2)
	}
	// An even older request is ignored entirely.
	if _, dup := sm.Apply(kv.Op{Kind: kv.OpPut, Key: "k", Value: "old", ClientID: "c", Seq: 1}); !dup {
		t.Fatal("stale request was executed")
	}
	if _, ok := sm.Read("k"); ok {
		t.Fatal("stale put resurrected the key")
	}
	// Requests without a client ID are never de-duplicated.
	sm.Apply(kv.Op{Kind: kv.OpPut, Key: "n", Value: "1"})
	if _, dup := sm.Apply(kv.Op{Kind: kv.OpPut, Key: "n", Value: "1"}); dup {
		t.Fatal("anonymous request treated as duplicate")
	}
}

func TestStateMachineSnapshotKeepsSessions(t *testing.T) {
	sm := kv.NewStateMachine()
	sm.Apply(kv.Op{Kind: kv.OpPut, Key: "a", Value: "1", ClientID: "c", Seq: 7})
	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := kv.NewStateMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if v, ok := restored.Read("a"); !ok || v != "1" {
		t.Fatalf("restored a=%q,%v", v, ok)
	}
	if _, dup := restored.Apply(kv.Op{Kind: kv.OpPut, Key: "a", Value: "2", ClientID: "c", Seq: 7}); !dup {
		t.Fatal("de-duplication state was lost in the snapshot")
	}
}

func TestPutGetDelete(t *testing.T) {
	c := newCluster(t, 3, 0)
	ctx := ctxFor(t, 10*time.Second)
	ck := c.clerk("client")

	if _, found, err := ck.Get(ctx, "x"); err != nil || found {
		t.Fatalf("get missing key: found=%v err=%v", found, err)
	}
	if err := ck.Put(ctx, "x", "1"); err != nil {
		t.Fatal(err)
	}
	if v, found, err := ck.Get(ctx, "x"); err != nil || !found || v != "1" {
		t.Fatalf("get x = %q,%v,%v", v, found, err)
	}
	if existed, err := ck.Delete(ctx, "x"); err != nil || !existed {
		t.Fatalf("delete x: existed=%v err=%v", existed, err)
	}
	if existed, err := ck.Delete(ctx, "x"); err != nil || existed {
		t.Fatalf("second delete: existed=%v err=%v", existed, err)
	}
	if _, found, _ := ck.Get(ctx, "x"); found {
		t.Fatal("deleted key is still there")
	}
}

func TestSubmitValidatesAndRedirects(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.leader()
	ctx := ctxFor(t, 5*time.Second)
	if _, err := c.server(leader).Submit(ctx, kv.Op{Kind: kv.OpPut}); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, err := c.server(leader).Submit(ctx, kv.Op{Kind: "incr", Key: "k"}); err == nil {
		t.Fatal("unknown op accepted")
	}
	// Followers learn the leader from its heartbeats and point clients to it.
	follower := leader%3 + 1
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := c.server(follower).Submit(ctx, kv.Op{Kind: kv.OpGet, Key: "k"})
		var nl *kv.NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("follower returned %v, want NotLeaderError", err)
		}
		if nl.Leader == leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower hint %d, want %d", nl.Leader, leader)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A request whose reply was lost is retried after another client has
// overwritten the key. The retry must not be applied a second time.
func TestRetriedRequestIsAppliedOnce(t *testing.T) {
	c := newCluster(t, 3, 0)
	ctx := ctxFor(t, 10*time.Second)
	opA := kv.Op{Kind: kv.OpPut, Key: "x", Value: "from-a", ClientID: "a", Seq: 1}

	submit := func(op kv.Op) {
		t.Helper()
		for ctx.Err() == nil {
			for id := 1; id <= 3; id++ {
				if _, err := c.server(id).Submit(ctx, op); err == nil {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("request never succeeded")
	}
	submit(opA)
	if err := c.clerk("b").Put(ctx, "x", "from-b"); err != nil {
		t.Fatal(err)
	}
	submit(opA) // the late retry
	if v, _, err := c.clerk("reader").Get(ctx, "x"); err != nil || v != "from-b" {
		t.Fatalf("x = %q (%v), want from-b: the retried put was applied twice", v, err)
	}
}

// The clerk throws away half of the successful replies and retries; results
// must still match a sequential model, including Delete's "existed" flag,
// which would flip to false if a retried delete ran twice.
func TestLossyClientNetworkIsDeduplicated(t *testing.T) {
	c := newCluster(t, 3, 0)
	ctx := ctxFor(t, 20*time.Second)
	ck := c.clerk("lossy")
	ck.LoseReplyRate = 0.5
	model := map[string]string{}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("k%d", rng.IntN(3))
		switch rng.IntN(3) {
		case 0:
			v := fmt.Sprintf("v%d", i)
			if err := ck.Put(ctx, key, v); err != nil {
				t.Fatal(err)
			}
			model[key] = v
		case 1:
			existed, err := ck.Delete(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if _, want := model[key]; existed != want {
				t.Fatalf("op %d: delete(%s) existed=%v, model says %v", i, key, existed, want)
			}
			delete(model, key)
		default:
			v, found, err := ck.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if want, ok := model[key]; found != ok || v != want {
				t.Fatalf("op %d: get(%s) = %q,%v, model says %q,%v", i, key, v, found, want, ok)
			}
		}
	}
	if ck.Retries == 0 {
		t.Fatal("no request was retried; the test exercised nothing")
	}
	t.Logf("%d retries", ck.Retries)
}

func TestMinorityPartitionCannotServe(t *testing.T) {
	c := newCluster(t, 5, 0)
	ctx := ctxFor(t, 20*time.Second)
	if err := c.clerk("setup").Put(ctx, "x", "1"); err != nil {
		t.Fatal(err)
	}
	old := c.leader()
	minority := []int{old, old%5 + 1}
	var majority []int
	for id := 1; id <= 5; id++ {
		if id != minority[0] && id != minority[1] {
			majority = append(majority, id)
		}
	}
	c.net.Partition(minority, majority)

	mctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	if err := c.clerk("stuck", minority...).Put(mctx, "x", "minority"); err == nil {
		t.Fatal("write succeeded on the minority side")
	}
	if err := c.clerk("majority", majority...).Put(ctx, "x", "2"); err != nil {
		t.Fatal(err)
	}
	// The deposed leader may still believe it leads, but reads go through
	// the log, so it cannot answer with the stale value.
	rctx, rcancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer rcancel()
	if v, _, err := c.clerk("stale-reader", minority...).Get(rctx, "x"); err == nil {
		t.Fatalf("minority side answered a read (x = %q)", v)
	}

	c.net.Heal()
	reader := c.clerk("reader")
	for i := 0; i < 3; i++ {
		if v, _, err := reader.Get(ctx, "x"); err != nil || v != "2" {
			t.Fatalf("x = %q (%v), want 2", v, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func TestSnapshotsBoundLogAndSurviveRestart(t *testing.T) {
	const threshold = 1500
	c := newCluster(t, 3, threshold)
	ctx := ctxFor(t, 30*time.Second)
	ck := c.clerk("writer")

	leader := c.leader()
	lagging := leader%3 + 1
	c.net.Disconnect(lagging)
	for i := 0; i < 200; i++ {
		if err := ck.Put(ctx, fmt.Sprintf("key-%d", i%20), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for id := 1; id <= 3; id++ {
		if id == lagging {
			continue
		}
		node := c.server(id).Node()
		if st := node.Status(); st.FirstIndex == 0 {
			t.Fatalf("node %d never compacted (last index %d)", id, st.LastIndex)
		}
		if size := node.StateSize(); size > 3*threshold {
			t.Fatalf("node %d persisted state is %d bytes; snapshots are not bounding the log", id, size)
		}
	}

	// The lagging node is too far behind for AppendEntries alone.
	c.net.Connect(lagging)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, _ := c.server(lagging).LocalRead("key-19"); v == "v199" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lagging node did not catch up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := c.server(lagging).Node().Status(); st.FirstIndex == 0 {
		t.Fatal("lagging node caught up without a snapshot")
	}

	for id := 1; id <= 3; id++ {
		c.crash(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	reader := c.clerk("reader")
	for k := 0; k < 20; k++ {
		want := fmt.Sprintf("v%d", 180+k)
		if v, _, err := reader.Get(ctx, fmt.Sprintf("key-%d", k)); err != nil || v != want {
			t.Fatalf("after restart key-%d = %q (%v), want %s", k, v, err, want)
		}
	}
}

// Five clients run concurrent puts, gets and deletes on four keys while the
// network drops and reorders messages, partitions come and go, and servers
// crash and restart. The recorded history must be linearizable.
func TestLinearizableUnderFaults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int
	}{
		{"log only", 0},
		{"with snapshots", 2000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const nodes, clients, opsPerClient, keys = 5, 5, 48, 4
			c := newCluster(t, nodes, tc.threshold)
			c.net.SetOptions(simnet.Options{
				DropRate:     0.05,
				MaxDelay:     10 * time.Millisecond,
				ReorderRate:  0.05,
				ReorderDelay: 100 * time.Millisecond,
			})
			ctx := ctxFor(t, 60*time.Second)
			start := time.Now()

			histories := make([][]lincheck.Op, clients)
			errs := make(chan error, clients)
			retries := make([]int, clients)
			var wg sync.WaitGroup
			for ci := 0; ci < clients; ci++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ck := c.clerk(fmt.Sprintf("client-%d", ci))
					rng := rand.New(rand.NewPCG(uint64(c.seed), uint64(ci)))
					for j := 0; j < opsPerClient; j++ {
						key := fmt.Sprintf("k%d", (ci+j)%keys)
						op := lincheck.Op{Client: ci, Key: key, Call: time.Since(start)}
						var err error
						switch r := rng.IntN(10); {
						case r < 4:
							op.Kind, op.Value = lincheck.Put, fmt.Sprintf("c%d-%d", ci, j)
							err = ck.Put(ctx, key, op.Value)
						case r < 5:
							op.Kind = lincheck.Delete
							op.Found, err = ck.Delete(ctx, key)
						default:
							op.Kind = lincheck.Get
							op.Value, op.Found, err = ck.Get(ctx, key)
						}
						op.Return = time.Since(start)
						if err != nil {
							errs <- fmt.Errorf("client %d op %d: %w", ci, j, err)
							return
						}
						histories[ci] = append(histories[ci], op)
						time.Sleep(time.Duration(rng.IntN(30)) * time.Millisecond)
					}
					retries[ci] = ck.Retries
				}()
			}
			clientsDone := make(chan struct{})
			go func() { wg.Wait(); close(clientsDone) }()

			faults := c.chaos(clientsDone, 4*time.Second)
			<-clientsDone
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}

			var all []lincheck.Op
			for _, h := range histories {
				all = append(all, h...)
			}
			totalRetries := 0
			for _, r := range retries {
				totalRetries += r
			}
			t.Logf("%d ops in %v, %d faults injected, %d client retries, network %+v",
				len(all), time.Since(start).Round(time.Millisecond), faults, totalRetries, c.net.Stats())
			if err := lincheck.Check(all); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// chaos injects partitions, heals, crashes and restarts until done is closed
// or limit elapses, then repairs everything. It returns the number of faults.
func (c *cluster) chaos(done <-chan struct{}, limit time.Duration) int {
	rng := rand.New(rand.NewPCG(uint64(c.seed), 99))
	stopAt := time.After(limit)
	faults := 0
	crashed := map[int]bool{}
loop:
	for {
		select {
		case <-done:
			break loop
		case <-stopAt:
			break loop
		case <-time.After(time.Duration(300+rng.IntN(600)) * time.Millisecond):
		}
		switch r := rng.IntN(8); {
		case r < 3: // random majority/minority split
			perm := rng.Perm(c.n)
			var majority, minority []int
			for i, p := range perm {
				if i <= c.n/2 {
					majority = append(majority, p+1)
				} else {
					minority = append(minority, p+1)
				}
			}
			c.net.Partition(majority, minority)
			faults++
		case r < 5: // crash the leader or a random node, at most two down
			id := rng.IntN(c.n) + 1
			if r == 4 {
				for i := 1; i <= c.n; i++ {
					if srv := c.server(i); srv != nil && srv.Node().Status().Role == "leader" {
						id = i
					}
				}
			}
			if len(crashed) < 2 && !crashed[id] {
				c.crash(id)
				crashed[id] = true
				faults++
			}
		default: // repair: heal the network and restart one node
			c.net.Heal()
			for id := 1; id <= c.n; id++ {
				if crashed[id] {
					c.start(id)
					delete(crashed, id)
					break
				}
			}
		}
	}
	c.net.Heal()
	for id := range crashed {
		c.start(id)
	}
	return faults
}
