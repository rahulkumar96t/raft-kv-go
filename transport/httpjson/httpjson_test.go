package httpjson_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/raft"
	"github.com/rahulkumar96t/raft-kv-go/transport/httpjson"
)

// Three real raft.Nodes talking JSON over loopback HTTP elect a leader and
// replicate a command to everyone.
func TestClusterOverHTTP(t *testing.T) {
	const n = 3
	muxes := make([]*http.ServeMux, n+1)
	peers := map[int]string{}
	for id := 1; id <= n; id++ {
		muxes[id] = http.NewServeMux()
		srv := httptest.NewServer(muxes[id])
		t.Cleanup(srv.Close)
		peers[id] = srv.URL
	}

	var mu sync.Mutex
	applied := map[int][]string{}
	nodes := make([]*raft.Node, n+1)
	for id := 1; id <= n; id++ {
		applyCh := make(chan raft.ApplyMsg)
		node, err := raft.New(raft.Config{ID: id, Peers: []int{1, 2, 3}, Seed: int64(id)},
			httpjson.NewClient(peers, nil), raft.NewMemoryPersister(), applyCh)
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = node
		httpjson.Register(muxes[id], node)
		done := make(chan struct{})
		t.Cleanup(func() { node.Stop(); close(done) })
		go func() {
			for {
				select {
				case <-done:
					return
				case msg := <-applyCh:
					if msg.CommandValid && len(msg.Command) > 0 {
						mu.Lock()
						applied[id] = append(applied[id], string(msg.Command))
						mu.Unlock()
					}
				}
			}
		}()
	}

	deadline := time.Now().Add(10 * time.Second)
	proposed := false
	for time.Now().Before(deadline) {
		if !proposed {
			for id := 1; id <= n; id++ {
				if _, _, ok := nodes[id].Propose([]byte("over-http")); ok {
					proposed = true
					break
				}
			}
		}
		mu.Lock()
		all := len(applied[1]) == 1 && len(applied[2]) == 1 && len(applied[3]) == 1
		for id := 1; all && id <= n; id++ {
			if applied[id][0] != "over-http" {
				t.Errorf("node %d applied %q", id, applied[id][0])
			}
		}
		mu.Unlock()
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("command not replicated to all nodes (proposed=%v, applied=%v)", proposed, applied)
}

type stubHandler struct{}

func (stubHandler) HandleRequestVote(a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{Term: a.Term + 1}, nil
}
func (stubHandler) HandleAppendEntries(*raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, raft.ErrStopped
}
func (stubHandler) HandleInstallSnapshot(*raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

func TestClientAndHandlerErrors(t *testing.T) {
	mux := http.NewServeMux()
	httpjson.Register(mux, stubHandler{})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := httpjson.NewClient(map[int]string{2: srv.URL}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := c.RequestVote(ctx, 2, &raft.RequestVoteArgs{Term: 4})
	if err != nil || reply.Term != 5 {
		t.Fatalf("reply %+v, err %v", reply, err)
	}
	if _, err := c.AppendEntries(ctx, 2, &raft.AppendEntriesArgs{}); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("stopped node should surface as 503, got %v", err)
	}
	if _, err := c.RequestVote(ctx, 9, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("unknown peer should fail")
	}

	resp, err := http.Post(srv.URL+httpjson.PathRequestVote, "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed body: status %d, want 400", resp.StatusCode)
	}
}
