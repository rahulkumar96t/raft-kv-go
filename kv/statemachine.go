// Package kv is a linearizable key-value store replicated with Raft. Every
// operation, reads included, goes through the Raft log, and each client
// request carries a (ClientID, Seq) pair so that a retried request is applied
// at most once.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
)

// OpKind is the type of a key-value operation.
type OpKind string

const (
	OpPut    OpKind = "put"
	OpGet    OpKind = "get"
	OpDelete OpKind = "delete"
)

// Op is the command stored in the Raft log.
type Op struct {
	Kind  OpKind `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// ClientID and Seq identify the request for de-duplication. A client
	// issues requests one at a time with increasing Seq values and retries a
	// request with the same Seq until it gets an answer. An empty ClientID
	// opts out of de-duplication.
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

func (op Op) validate() error {
	switch op.Kind {
	case OpPut, OpGet, OpDelete:
	default:
		return fmt.Errorf("kv: unknown operation %q", op.Kind)
	}
	if op.Key == "" {
		return errors.New("kv: empty key")
	}
	if op.ClientID != "" && op.Seq == 0 {
		return errors.New("kv: requests with a client ID need a sequence number >= 1")
	}
	return nil
}

// Result is the outcome of an operation. For Get, Value/Found describe the
// key; for Delete, Found reports whether the key existed.
type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"`
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// StateMachine is the deterministic part of the store: the data plus, per
// client, the last applied sequence number and its result. Both live in
// snapshots, so de-duplication survives compaction and restarts.
type StateMachine struct {
	data     map[string]string
	sessions map[string]session
}

// NewStateMachine returns an empty store.
func NewStateMachine() *StateMachine {
	return &StateMachine{data: map[string]string{}, sessions: map[string]session{}}
}

// Apply executes op unless it is a duplicate, in which case the cached
// result of the original execution is returned and duplicate is true.
func (sm *StateMachine) Apply(op Op) (res Result, duplicate bool) {
	if op.ClientID != "" {
		if s, ok := sm.sessions[op.ClientID]; ok && op.Seq <= s.Seq {
			if op.Seq == s.Seq {
				return s.Result, true
			}
			// Older than the last request: the client has already moved on
			// and nobody is waiting for this answer.
			return Result{}, true
		}
	}
	switch op.Kind {
	case OpPut:
		sm.data[op.Key] = op.Value
	case OpGet:
		res.Value, res.Found = sm.data[op.Key]
	case OpDelete:
		_, res.Found = sm.data[op.Key]
		delete(sm.data, op.Key)
	}
	if op.ClientID != "" {
		sm.sessions[op.ClientID] = session{Seq: op.Seq, Result: res}
	}
	return res, false
}

// Read returns the local value of key without going through Raft.
func (sm *StateMachine) Read(key string) (string, bool) {
	v, ok := sm.data[key]
	return v, ok
}

type snapshotData struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

// Snapshot serialises the state (JSON sorts map keys, so it is deterministic).
func (sm *StateMachine) Snapshot() ([]byte, error) {
	return json.Marshal(snapshotData{Data: sm.data, Sessions: sm.sessions})
}

// Restore replaces the state with a snapshot.
func (sm *StateMachine) Restore(b []byte) error {
	var s snapshotData
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	if s.Sessions == nil {
		s.Sessions = map[string]session{}
	}
	sm.data, sm.sessions = s.Data, s.Sessions
	return nil
}
