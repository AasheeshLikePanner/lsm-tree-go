package lsm_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/wal"
)

func TestCrashRecovery(t *testing.T) {
	if os.Getenv("LSM_CRASH_CHILD") == "1" {
		crashChild()
		return
	}

	dir, err := os.MkdirTemp("", "lsm-crash")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(os.Args[0],
		"-test.run=^TestCrashRecovery$",
		"-test.v",
	)
	cmd.Env = append(os.Environ(),
		"LSM_CRASH_CHILD=1",
		"LSM_CRASH_DIR="+dir,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill child: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		t.Logf("child exited with error (expected: killed with SIGKILL): %v", err)
	} else {
		t.Log("child exited cleanly")
	}

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	keysFound := 0
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%08d", i)
		want := fmt.Sprintf("val-%08d", i)
		got, ok := db.Get(key)
		if !ok {
			break
		}
		if got != want {
			t.Fatalf("key %q: got %q, want %q (corruption at key %d)", key, got, want, i)
		}
		keysFound++
	}

	if keysFound < 10000 {
		t.Fatalf("expected at least 10000 recoverable keys, got %d (child may have been killed too early)", keysFound)
	}

	t.Logf("recovered %d keys after SIGKILL", keysFound)

	for i := keysFound; i < keysFound+1000; i++ {
		key := fmt.Sprintf("key-%08d", i)
		got, ok := db.Get(key)
		if !ok {
			continue
		}
		want := fmt.Sprintf("val-%08d", i)
		if got != want {
			t.Fatalf("phantom key %q with wrong value %q (expected %q or absent)", key, got, want)
		}
	}
}

func crashChild() {
	dir := os.Getenv("LSM_CRASH_DIR")
	if dir == "" {
		fmt.Fprintf(os.Stderr, "LSM_CRASH_DIR not set\n")
		os.Exit(1)
	}

	sentinel := filepath.Join(dir, ".crash_started")
	os.WriteFile(sentinel, []byte{}, 0644)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open: %v\n", err)
		os.Exit(1)
	}

	batch := make([]lsm.KeyValue, 0, 1000)
	for i := 0; ; i++ {
		batch = append(batch, lsm.KeyValue{
			Key:   fmt.Sprintf("key-%08d", i),
			Value: fmt.Sprintf("val-%08d", i),
		})
		if len(batch) >= 1000 {
			if err := db.PutBatch(batch); err != nil {
				fmt.Fprintf(os.Stderr, "child: PutBatch: %v\n", err)
				os.Exit(1)
			}
			batch = batch[:0]

			time.Sleep(time.Millisecond)
		}
		if i >= 200000 {

			break
		}
	}
	if len(batch) > 0 {
		db.PutBatch(batch)
	}
	db.Close()
	os.Exit(0)
}

func TestCrashWALDirect(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm-crash-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const N = 1000
	walPath := filepath.Join(dir, "wal.log")
	entries := make([]lsm.KeyValue, N)
	for i := 0; i < N; i++ {
		entries[i] = lsm.KeyValue{
			Key:   fmt.Sprintf("key-%08d", i),
			Value: fmt.Sprintf("val-%08d", i),
		}
	}

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PutBatch(entries); err != nil {
		t.Fatal(err)
	}

	db.Close()

	w2, err := wal.New(walPath, wal.SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != N {
		t.Fatalf("expected %d replayed entries, got %d", N, len(replayed))
	}
	for i := 0; i < N; i++ {
		if replayed[i].Key != entries[i].Key || replayed[i].Value != entries[i].Value {
			t.Fatalf("entry %d: got key=%q val=%q, want key=%q val=%q",
				i, replayed[i].Key, replayed[i].Value, entries[i].Key, entries[i].Value)
		}
	}
	t.Logf("WAL direct replay recovered %d entries", len(replayed))

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
		want := fmt.Sprintf("val-%08d", i)
		got, ok := db2.Get(key)
		if !ok {
			t.Fatalf("key %q not found after LSM reopen", key)
		}
		if got != want {
			t.Fatalf("key %q after reopen: got %q, want %q", key, got, want)
		}
	}
	t.Log("LSM reopen recovery: all entries verified")
}
