package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryPersisterCopyIsIndependent(t *testing.T) {
	p := NewMemoryPersister()
	_ = p.Save([]byte("s1"), []byte("snap1"))
	c := p.Copy()
	_ = p.Save([]byte("s2"), []byte("snap2"))
	state, snap, _ := c.Load()
	if string(state) != "s1" || string(snap) != "snap1" {
		t.Fatalf("copy sees %q/%q, want s1/snap1", state, snap)
	}
}

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s, snap, err := p.Load(); err != nil || s != nil || snap != nil {
		t.Fatalf("fresh dir: %q %q %v", s, snap, err)
	}
	for _, c := range []struct{ state, snap []byte }{
		{[]byte("first state"), nil},
		{[]byte("second state"), bytes.Repeat([]byte{7}, 10_000)},
		{[]byte{}, []byte("only snapshot")},
	} {
		if err := p.Save(c.state, c.snap); err != nil {
			t.Fatal(err)
		}
		reopened, _ := NewFilePersister(dir)
		s, snap, err := reopened.Load()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(s, c.state) || !bytes.Equal(snap, c.snap) {
			t.Fatalf("round trip mismatch: got %d/%d bytes, want %d/%d", len(s), len(snap), len(c.state), len(c.snap))
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "raft.dat.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
}

func TestFilePersisterDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir)
	if err := p.Save([]byte("state"), []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raft.dat")
	b, _ := os.ReadFile(path)
	b[len(fileMagic)+10] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Load(); err == nil {
		t.Fatal("expected a checksum error")
	}
}

func TestNodeStateSurvivesFilePersister(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir)
	n := idleNode(t, p)
	withLog(n, 1, 2, 2)
	n.votedFor = 2
	n.persist()

	p2, _ := NewFilePersister(dir)
	restarted := idleNode(t, p2)
	if restarted.currentTerm != 2 || restarted.votedFor != 2 || !equalTerms(logTerms(restarted), []uint64{1, 2, 2}) {
		t.Fatalf("restored term=%d vote=%d log=%v", restarted.currentTerm, restarted.votedFor, logTerms(restarted))
	}
	if string(restarted.entries[3].Command) != string([]byte{3}) {
		t.Fatalf("command not restored: %v", restarted.entries[3].Command)
	}
}
