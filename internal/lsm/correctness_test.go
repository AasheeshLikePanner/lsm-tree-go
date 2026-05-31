package lsm_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/wal"
)

func TestCorrectnessWriteReadVerify(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm-correctness")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	const N = 50000

	{
		batch := make([]lsm.KeyValue, 0, 1000)
		for i := 0; i < N; i++ {
			batch = append(batch, lsm.KeyValue{
				Key:   fmt.Sprintf("key-%08d", i),
				Value: fmt.Sprintf("val-%08d", i),
			})
			if len(batch) >= 1000 {
				if err := db.PutBatch(batch); err != nil {
					t.Fatalf("PutBatch at i=%d: %v", i, err)
				}
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			db.PutBatch(batch)
		}
	}

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		want := fmt.Sprintf("val-%08d", i)
		got, ok := db.Get(key)
		if !ok {
			t.Fatalf("key %q not found after write", key)
		}
		if got != want {
			t.Fatalf("key %q: got %q, want %q", key, got, want)
		}
	}

	{
		batch := make([]lsm.KeyValue, 0, 1000)
		for i := 0; i < N/2; i++ {
			batch = append(batch, lsm.KeyValue{
				Key:   fmt.Sprintf("key-%08d", i),
				Value: fmt.Sprintf("overwrite-%08d", i+N),
			})
			if len(batch) >= 1000 {
				if err := db.PutBatch(batch); err != nil {
					t.Fatalf("overwrite PutBatch at i=%d: %v", i, err)
				}
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			db.PutBatch(batch)
		}
	}

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		var want string
		if i < N/2 {
			want = fmt.Sprintf("overwrite-%08d", i+N)
		} else {
			want = fmt.Sprintf("val-%08d", i)
		}
		got, ok := db.Get(key)
		if !ok {
			t.Fatalf("key %q not found after overwrite", key)
		}
		if got != want {
			t.Fatalf("key %q: got %q, want %q", key, got, want)
		}
	}

	db.Close()

	db2, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		var want string
		if i < N/2 {
			want = fmt.Sprintf("overwrite-%08d", i+N)
		} else {
			want = fmt.Sprintf("val-%08d", i)
		}
		got, ok := db2.Get(key)
		if !ok {
			t.Fatalf("key %q not found after reopen", key)
		}
		if got != want {
			t.Fatalf("key %q after reopen: got %q, want %q", key, got, want)
		}
	}
}
