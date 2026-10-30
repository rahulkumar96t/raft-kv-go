package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/api"
	"github.com/rahulkumar96t/raft-kv-go/kv"
	"github.com/rahulkumar96t/raft-kv-go/raft"
	"github.com/rahulkumar96t/raft-kv-go/transport/httpjson"
)

// startCluster runs three kv servers, each serving Raft RPCs and the client
// API on its own httptest server, exactly as cmd/raftkv wires them.
func startCluster(t *testing.T) map[int]string {
	t.Helper()
	muxes := map[int]*http.ServeMux{}
	urls := map[int]string{}
	for id := 1; id <= 3; id++ {
		muxes[id] = http.NewServeMux()
		srv := httptest.NewServer(muxes[id])
		t.Cleanup(srv.Close)
		urls[id] = srv.URL
	}
	for id := 1; id <= 3; id++ {
		s, err := kv.NewServer(kv.Config{Raft: raft.Config{ID: id, Peers: []int{1, 2, 3}, Seed: int64(id)}},
			httpjson.NewClient(urls, nil), raft.NewMemoryPersister())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Stop)
		httpjson.Register(muxes[id], s.Node())
		api.Register(muxes[id], s, id, urls)
	}
	return urls
}

func status(t *testing.T, url string) raft.Status {
	t.Helper()
	resp, err := http.Get(url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st raft.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// waitForLeader returns the leader once a follower also knows who it is.
func waitForLeader(t *testing.T, urls map[int]string) (leader, follower int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, u := range urls {
			st := status(t, u)
			if st.Role == "follower" && st.Leader != 0 && status(t, urls[st.Leader]).Role == "leader" {
				return st.Leader, id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no stable leader")
	return 0, 0
}

func do(t *testing.T, c *http.Client, method, url, body string, headers ...string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func TestHTTPAPI(t *testing.T) {
	urls := startCluster(t)
	leader, follower := waitForLeader(t, urls)
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// A follower redirects writes to the leader with 307 (method and body kept).
	code, _, hdr := do(t, noRedirect, http.MethodPut, urls[follower]+"/kv/greeting", "hello")
	if code != http.StatusTemporaryRedirect || hdr.Get("Location") != urls[leader]+"/kv/greeting" {
		t.Fatalf("follower PUT: %d Location=%q, want 307 to %s", code, hdr.Get("Location"), urls[leader])
	}

	// The default client follows the redirect.
	if code, body, _ := do(t, http.DefaultClient, http.MethodPut, urls[follower]+"/kv/greeting", "hello"); code != http.StatusNoContent {
		t.Fatalf("PUT via follower: %d %s", code, body)
	}
	if code, body, _ := do(t, http.DefaultClient, http.MethodGet, urls[follower]+"/kv/greeting", ""); code != http.StatusOK || body != "hello" {
		t.Fatalf("GET: %d %q", code, body)
	}
	if code, _, _ := do(t, http.DefaultClient, http.MethodDelete, urls[leader]+"/kv/greeting", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE: %d", code)
	}
	if code, _, _ := do(t, http.DefaultClient, http.MethodGet, urls[leader]+"/kv/greeting", ""); code != http.StatusNotFound {
		t.Fatalf("GET after delete: %d, want 404", code)
	}
	if code, _, _ := do(t, http.DefaultClient, http.MethodDelete, urls[leader]+"/kv/greeting", ""); code != http.StatusNotFound {
		t.Fatalf("second DELETE: %d, want 404", code)
	}

	// Idempotent retries with X-Client-Id / X-Request-Seq.
	put := func(value, client, seq string) int {
		code, _, _ := do(t, http.DefaultClient, http.MethodPut, urls[leader]+"/kv/x", value, api.HeaderClientID, client, api.HeaderSeq, seq)
		return code
	}
	if put("a1", "alice", "1") != http.StatusNoContent || put("b1", "bob", "1") != http.StatusNoContent {
		t.Fatal("puts failed")
	}
	if put("a1", "alice", "1") != http.StatusNoContent {
		t.Fatal("retry failed")
	}
	if _, body, _ := do(t, http.DefaultClient, http.MethodGet, urls[leader]+"/kv/x", ""); body != "b1" {
		t.Fatalf("x = %q after alice's retry, want b1 (retry must not re-apply)", body)
	}
	if code, _, _ := do(t, http.DefaultClient, http.MethodPut, urls[leader]+"/kv/x", "v", api.HeaderClientID, "carol"); code != http.StatusBadRequest {
		t.Fatalf("client ID without sequence: %d, want 400", code)
	}

	if st := status(t, urls[leader]); st.Role != "leader" || st.CommitIndex < 5 {
		t.Fatalf("leader status %+v", st)
	}
}
