package wal

import (
	"os"
	"testing"

	"github.com/aasheesh/lsmtree/internal/memtable"
)

func TestWALAppendAndReplay(t *testing.T) {
	path := t.TempDir() + "/test.wal"

	w, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}

	entries := []memtable.Entry{
		{Key: "a", Value: "1", Timestamp: 100},
		{Key: "b", Value: "2", Timestamp: 200},
		{Key: "c", Value: "3", Timestamp: 300, Tombstone: true},
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w2, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(replayed))
	}

	for i, e := range entries {
		r := replayed[i]
		if r.Key != e.Key || r.Value != e.Value || r.Timestamp != e.Timestamp || r.Tombstone != e.Tombstone {
			t.Fatalf("entry %d mismatch: got %+v, expected %+v", i, r, e)
		}
	}
}

func TestWALReplayEmpty(t *testing.T) {
	path := t.TempDir() + "/empty.wal"
	w, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(replayed))
	}
}

func TestWALReset(t *testing.T) {
	path := t.TempDir() + "/reset.wal"
	w, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}

	w.Append(memtable.Entry{Key: "k", Value: "v", Timestamp: 1})

	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}

	w.Append(memtable.Entry{Key: "k2", Value: "v2", Timestamp: 2})
	w.Close()

	w2, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].Key != "k2" {
		t.Fatalf("expected 1 entry with key 'k2', got %+v", replayed)
	}
}

func TestWALFsync(t *testing.T) {
	path := t.TempDir() + "/fsync.wal"
	w, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Append(memtable.Entry{Key: "sync", Value: "test", Timestamp: 1}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("WAL file is empty after append+sync")
	}
}

func TestWALAsync(t *testing.T) {
	path := t.TempDir() + "/async.wal"
	w, err := New(path, SyncAsync)
	if err != nil {
		t.Fatal(err)
	}

	n := 10000
	for i := 0; i < n; i++ {
		if err := w.Append(memtable.Entry{Key: string(rune(i)), Value: "v", Timestamp: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w2, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != n {
		t.Fatalf("expected %d entries, got %d", n, len(replayed))
	}
}

func TestWALPeriodic(t *testing.T) {
	path := t.TempDir() + "/periodic.wal"
	w, err := New(path, SyncPeriodic)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 250; i++ {
		if err := w.Append(memtable.Entry{Key: string(rune(i)), Value: "v", Timestamp: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w2, err := New(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 250 {
		t.Fatalf("expected 250 entries, got %d", len(replayed))
	}
}
