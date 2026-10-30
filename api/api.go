// Package api exposes a kv.Server over HTTP.
//
//	PUT    /kv/{key}   body = value         204, or 307 to the leader
//	GET    /kv/{key}                        200 value | 404
//	DELETE /kv/{key}                        204 | 404
//	GET    /status                          node status as JSON
//
// Optional headers X-Client-Id and X-Request-Seq make a request idempotent:
// retrying it with the same pair returns the original result instead of
// applying it twice.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/kv"
)

const (
	HeaderClientID = "X-Client-Id"
	HeaderSeq      = "X-Request-Seq"
	HeaderLeader   = "X-Raft-Leader"

	maxValueBytes = 1 << 20
)

type handler struct {
	srv     *kv.Server
	self    int
	peers   map[int]string // node ID -> base URL, for redirects
	timeout time.Duration
}

// Register mounts the client API on mux. peers maps node IDs to base URLs
// (used to redirect clients to the leader).
func Register(mux *http.ServeMux, srv *kv.Server, self int, peers map[int]string) {
	h := &handler{srv: srv, self: self, peers: peers, timeout: 3 * time.Second}
	mux.HandleFunc("GET /kv/{key}", h.get)
	mux.HandleFunc("PUT /kv/{key}", h.put)
	mux.HandleFunc("DELETE /kv/{key}", h.del)
	mux.HandleFunc("GET /status", h.status)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	h.submit(w, r, kv.Op{Kind: kv.OpGet, Key: r.PathValue("key")})
}

func (h *handler) del(w http.ResponseWriter, r *http.Request) {
	h.submit(w, r, kv.Op{Kind: kv.OpDelete, Key: r.PathValue("key")})
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}
	h.submit(w, r, kv.Op{Kind: kv.OpPut, Key: r.PathValue("key"), Value: string(value)})
}

func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.srv.Node().Status())
}

func (h *handler) submit(w http.ResponseWriter, r *http.Request, op kv.Op) {
	if id := r.Header.Get(HeaderClientID); id != "" {
		seq, err := strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
		if err != nil || seq == 0 {
			http.Error(w, HeaderSeq+" must be a positive integer when "+HeaderClientID+" is set", http.StatusBadRequest)
			return
		}
		op.ClientID, op.Seq = id, seq
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	res, err := h.srv.Submit(ctx, op)

	var notLeader *kv.NotLeaderError
	switch {
	case err == nil:
		w.Header().Set(HeaderLeader, strconv.Itoa(h.self))
		switch {
		case op.Kind == kv.OpGet && res.Found:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, res.Value)
		case op.Kind == kv.OpPut || res.Found:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "key not found", http.StatusNotFound)
		}
	case errors.As(err, &notLeader):
		if url, ok := h.peers[notLeader.Leader]; ok && notLeader.Leader != h.self {
			// 307 keeps the method and body, so `curl -L` just works.
			w.Header().Set(HeaderLeader, strconv.Itoa(notLeader.Leader))
			http.Redirect(w, r, url+r.URL.RequestURI(), http.StatusTemporaryRedirect)
			return
		}
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader known yet, retry shortly", http.StatusServiceUnavailable)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kv.ErrUnknownOutcome):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "outcome unknown (timeout or leadership change); retrying with the same "+
			HeaderClientID+"/"+HeaderSeq+" is safe", http.StatusServiceUnavailable)
	case errors.Is(err, kv.ErrStopped):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
