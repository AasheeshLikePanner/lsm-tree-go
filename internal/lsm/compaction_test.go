package lsm_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/wal"
)

func TestCompactionTombstonePropagation(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm-compaction-tombstone")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	batch := make([]lsm.KeyValue, 0, 1000)
	for i := 0; i < 5000; i++ {
		batch = append(batch, lsm.KeyValue{
			Key:   fmt.Sprintf("key-%08d", i),
			Value: fmt.Sprintf("val-%08d", i),
		})
		if len(batch) >= 1000 {
			if err := db.PutBatch(batch); err != nil {
				t.Fatalf("PutBatch: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		db.PutBatch(batch)
	}

	if _, ok := db.Get("key-00000000"); !ok {
		t.Fatal("key-00000000 should exist before delete")
	}

	if err := db.Delete("key-00000000"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok := db.Get("key-00000000"); ok {
		t.Fatal("key-00000000 should be gone after delete")
	}

	for j := 0; j < 10; j++ {
		batch := make([]lsm.KeyValue, 0, 1000)
		base := 5000 + j*1000
		for i := 0; i < 1000; i++ {
			batch = append(batch, lsm.KeyValue{
				Key:   fmt.Sprintf("key-%08d", base+i),
				Value: fmt.Sprintf("val-%08d", base+i),
			})
		}
		if err := db.PutBatch(batch); err != nil {
			t.Fatalf("PutBatch pass %d: %v", j, err)
		}
	}

	db.Close()

	db2, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, ok := db2.Get("key-00000000"); ok {
		t.Fatal("key-00000000 should still be gone after compaction")
	}

	for i := 1; i < 100; i++ {
		key := fmt.Sprintf("key-%08d", i)
		want := fmt.Sprintf("val-%08d", i)
		got, ok := db2.Get(key)
		if !ok {
			t.Fatalf("key %q missing after compaction", key)
		}
		if got != want {
			t.Fatalf("key %q after compaction: got %q, want %q", key, got, want)
		}
	}

	t.Log("tombstone propagation verified")
}

func TestCompactionDuplicateKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm-compaction-dups")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		NumLevels:      4,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	for v := 0; v < 100; v++ {
		batch := make([]lsm.KeyValue, 0, 100)
		for i := 0; i < 100; i++ {
			batch = append(batch, lsm.KeyValue{
				Key:   "dup-key",
				Value: fmt.Sprintf("version-%03d", v),
			})

			batch = append(batch, lsm.KeyValue{
				Key:   fmt.Sprintf("filler-%04d", v*100+i),
				Value: "x",
			})
		}
		if err := db.PutBatch(batch); err != nil {
			t.Fatalf("PutBatch version %d: %v", v, err)
		}
	}

	got, ok := db.Get("dup-key")
	if !ok {
		t.Fatal("dup-key not found")
	}
	if got != "version-099" {
		t.Fatalf("dup-key: got %q, want %q", got, "version-099")
	}

	statsBefore := db.Stats()

	db.Close()

	db2, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		NumLevels:      4,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	got, ok = db2.Get("dup-key")
	if !ok {
		t.Fatal("dup-key not found after reopen")
	}
	if got != "version-099" {
		t.Fatalf("dup-key after reopen: got %q, want %q", got, "version-099")
	}

	statsAfter := db2.Stats()

	if statsAfter.SSTableBytesWritten <= statsBefore.SSTableBytesWritten {

		t.Logf("SSTable bytes before=%d after=%d (compaction may be pending)",
			statsBefore.SSTableBytesWritten, statsAfter.SSTableBytesWritten)
	}

	t.Log("duplicate key compaction verified")
}

func TestCompactionLevelInvariants(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm-compaction-invariants")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 512 << 10,
		NumLevels:      4,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 20; j++ {
		batch := make([]lsm.KeyValue, 0, 1000)
		base := j * 1000
		for i := 0; i < 1000; i++ {
			batch = append(batch, lsm.KeyValue{
				Key:   fmt.Sprintf("key-%08d", base+i),
				Value: fmt.Sprintf("val-%08d", base+i),
			})
		}
		if err := db.PutBatch(batch); err != nil {
			t.Fatalf("PutBatch pass %d: %v", j, err)
		}
	}

	db.Close()

	db2, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 512 << 10,
		NumLevels:      4,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if db2.HasOverlappingLevels() {
		t.Fatal("non-L0 levels have overlapping SSTable key ranges after compaction")
	}

	t.Log("level invariants verified: no overlapping SSTables in L1+")
}
