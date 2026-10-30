// Command raftkv runs one node of a Raft-replicated key-value store that
// speaks HTTP/JSON both to its peers and to clients.
//
//	raftkv --id 1 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rahulkumar96t/raft-kv-go/api"
	"github.com/rahulkumar96t/raft-kv-go/kv"
	"github.com/rahulkumar96t/raft-kv-go/raft"
	"github.com/rahulkumar96t/raft-kv-go/transport/httpjson"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv:", err)
		os.Exit(1)
	}
}

func run() error {
	id := flag.Int("id", 0, "ID of this node (must appear in --cluster)")
	cluster := flag.String("cluster", "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003",
		"comma-separated id=host:port of every member; this node listens on its own entry")
	dataDir := flag.String("data", "", "directory for durable Raft state (default ./data/node-<id>)")
	snapshotBytes := flag.Int("snapshot-bytes", 1<<20, "persisted Raft state size that triggers a snapshot; 0 disables")
	verbose := flag.Bool("v", false, "log debug events (elections, compaction)")
	flag.Parse()

	members, err := parseCluster(*cluster)
	if err != nil {
		return err
	}
	addr, ok := members[*id]
	if !ok {
		return fmt.Errorf("--id %d is not in --cluster %q", *id, *cluster)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node-%d", *id)
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	urls := map[int]string{}
	ids := make([]int, 0, len(members))
	for mid, a := range members {
		urls[mid] = "http://" + a
		ids = append(ids, mid)
	}
	sort.Ints(ids)

	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		return err
	}
	transport := httpjson.NewClient(urls, &http.Client{Timeout: 5 * time.Second})
	srv, err := kv.NewServer(kv.Config{
		Raft: raft.Config{
			ID:                 *id,
			Peers:              ids,
			ElectionTimeoutMin: 400 * time.Millisecond,
			ElectionTimeoutMax: 800 * time.Millisecond,
			HeartbeatInterval:  100 * time.Millisecond,
			RPCTimeout:         250 * time.Millisecond,
			Logger:             logger,
		},
		SnapshotThreshold: *snapshotBytes,
	}, transport, persister)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	httpjson.Register(mux, srv.Node())
	api.Register(mux, srv, *id, urls)
	httpServer := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()
	logger.Info("node started", "id", *id, "addr", addr, "data", *dataDir, "cluster", *cluster)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.Info("shutting down", "signal", sig.String())
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			srv.Stop()
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
	srv.Stop()
	return nil
}

// parseCluster parses "1=host:port,2=host:port".
func parseCluster(s string) (map[int]string, error) {
	members := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("bad cluster member %q, want id=host:port", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 1 {
			return nil, fmt.Errorf("bad node ID in %q", part)
		}
		if _, dup := members[id]; dup {
			return nil, fmt.Errorf("node ID %d listed twice", id)
		}
		members[id] = addr
	}
	if len(members) == 0 {
		return nil, errors.New("empty --cluster")
	}
	return members, nil
}
