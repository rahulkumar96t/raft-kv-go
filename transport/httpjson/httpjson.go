// Package httpjson carries Raft RPCs as JSON over HTTP POST requests.
//
//	POST /raft/request-vote      RequestVoteArgs     -> RequestVoteReply
//	POST /raft/append-entries    AppendEntriesArgs   -> AppendEntriesReply
//	POST /raft/install-snapshot  InstallSnapshotArgs -> InstallSnapshotReply
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rahulkumar96t/raft-kv-go/raft"
)

const (
	PathRequestVote     = "/raft/request-vote"
	PathAppendEntries   = "/raft/append-entries"
	PathInstallSnapshot = "/raft/install-snapshot"

	maxBodyBytes = 64 << 20 // snapshots travel in one request
)

// Client implements raft.Transport over HTTP.
type Client struct {
	peers map[int]string // node ID -> base URL, e.g. http://127.0.0.1:7001
	hc    *http.Client
}

var _ raft.Transport = (*Client)(nil)

// NewClient returns a transport that reaches peer IDs at the given base URLs.
// If hc is nil, http.DefaultClient is used (Raft's contexts bound each call).
func NewClient(peers map[int]string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{peers: peers, hc: hc}
}

func (c *Client) RequestVote(ctx context.Context, peer int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := c.post(ctx, peer, PathRequestVote, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *Client) AppendEntries(ctx context.Context, peer int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := c.post(ctx, peer, PathAppendEntries, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *Client) InstallSnapshot(ctx context.Context, peer int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	if err := c.post(ctx, peer, PathInstallSnapshot, args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *Client) post(ctx context.Context, peer int, path string, in, out any) error {
	base, ok := c.peers[peer]
	if !ok {
		return fmt.Errorf("httpjson: unknown peer %d", peer)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("httpjson: %s to node %d: %s: %s", path, peer, resp.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Register mounts the RPC endpoints for h on mux.
func Register(mux *http.ServeMux, h raft.RPCHandler) {
	mux.Handle("POST "+PathRequestVote, serve(h.HandleRequestVote))
	mux.Handle("POST "+PathAppendEntries, serve(h.HandleAppendEntries))
	mux.Handle("POST "+PathInstallSnapshot, serve(h.HandleInstallSnapshot))
}

func serve[A, R any](handle func(*A) (*R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var args A
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&args); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		reply, err := handle(&args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}
}
