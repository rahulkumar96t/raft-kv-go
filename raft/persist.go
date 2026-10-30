package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// Persister stores a node's durable state: the encoded Raft state (term,
// vote, log) and the latest snapshot. Save must replace both atomically, so
// that after a crash Load returns either the old pair or the new pair.
// Implementations may retain the slices they are given; callers must not
// modify them afterwards.
type Persister interface {
	Save(state, snapshot []byte) error
	Load() (state, snapshot []byte, err error)
}

// MemoryPersister keeps state in memory. It is what the tests use.
type MemoryPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// NewMemoryPersister returns an empty in-memory persister.
func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

// Save implements Persister.
func (p *MemoryPersister) Save(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state, p.snapshot = state, snapshot
	return nil
}

// Load implements Persister.
func (p *MemoryPersister) Load() ([]byte, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state, p.snapshot, nil
}

// Copy returns an independent persister holding the same data. Tests use it
// to model a crash: the restarted node sees exactly what had been saved and
// the stopped instance can no longer change it.
func (p *MemoryPersister) Copy() *MemoryPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemoryPersister{state: p.state, snapshot: p.snapshot}
}

const fileMagic = "RAFTKV01"

// FilePersister stores state and snapshot together in one file, replaced
// atomically with write-to-temp, fsync, rename and directory fsync. Keeping
// both in one file makes the pair consistent after a crash at the cost of
// rewriting the snapshot on every save (fine for small snapshots).
//
// Layout: magic | u64 len | state | u64 len | snapshot | crc32 (big endian).
type FilePersister struct {
	mu   sync.Mutex
	path string
}

// NewFilePersister stores state in dir/raft.dat, creating dir if needed.
func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}
	return &FilePersister{path: filepath.Join(dir, "raft.dat")}, nil
}

// Save implements Persister.
func (p *FilePersister) Save(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf := make([]byte, 0, len(fileMagic)+8+len(state)+8+len(snapshot)+4)
	buf = append(buf, fileMagic...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(state)))
	buf = append(buf, state...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(snapshot)))
	buf = append(buf, snapshot...)
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))

	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}
	// Make the rename itself durable. Not every platform supports fsync on a
	// directory, so this step is best effort.
	if d, err := os.Open(filepath.Dir(p.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Load implements Persister. A missing file means a fresh node.
func (p *FilePersister) Load() ([]byte, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	buf, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	corrupt := fmt.Errorf("raft: %s is corrupt", p.path)
	if len(buf) < len(fileMagic)+8+8+4 || string(buf[:len(fileMagic)]) != fileMagic {
		return nil, nil, corrupt
	}
	body, sum := buf[:len(buf)-4], binary.BigEndian.Uint32(buf[len(buf)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, nil, corrupt
	}
	rest := body[len(fileMagic):]
	next := func() ([]byte, bool) {
		if len(rest) < 8 {
			return nil, false
		}
		n := binary.BigEndian.Uint64(rest)
		rest = rest[8:]
		if uint64(len(rest)) < n {
			return nil, false
		}
		out := rest[:n:n]
		rest = rest[n:]
		return out, true
	}
	state, ok1 := next()
	snapshot, ok2 := next()
	if !ok1 || !ok2 || len(rest) != 0 {
		return nil, nil, corrupt
	}
	return state, snapshot, nil
}
