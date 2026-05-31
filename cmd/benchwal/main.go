package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/memtable"
	"github.com/aasheesh/lsmtree/internal/wal"
)

func main() {
	dir, _ := os.MkdirTemp("", "fsync-bench")
	defer os.RemoveAll(dir)

	fmt.Printf("Go %s | %d CPUs\n", runtime.Version(), runtime.NumCPU())

	f, _ := os.Create(dir + "/test.dat")
	buf := make([]byte, 37000)
	start := time.Now()
	for i := 0; i < 10000; i++ {
		f.Write(buf)
		f.Sync()
	}
	fmt.Printf("write+fsync 37KB x10000: %.0f/s\n", 10000/time.Since(start).Seconds())
	f.Close()

	w, _ := wal.New(dir+"/wal-bench", wal.SyncAlways)
	entries := make([]memtable.Entry, 1000)
	for i := range entries {
		entries[i] = memtable.Entry{
			Key:       fmt.Sprintf("key-%08d", i),
			Value:     fmt.Sprintf("val-%08d", i),
			Timestamp: int64(i),
		}
	}
	start = time.Now()
	n := 10000
	for i := 0; i < n; i++ {
		w.AppendBatch(entries)
	}
	fmt.Printf("WAL AppendBatch(1000) x%d: %.0f/s (%.0f writes/s)\n",
		n, float64(n)/time.Since(start).Seconds(),
		float64(n*1000)/time.Since(start).Seconds())
	w.Close()

	mt := memtable.New()
	start = time.Now()
	for i := 0; i < 1000000; i++ {
		mt.Put(fmt.Sprintf("key-%08d", i), fmt.Sprintf("val-%08d", i), int64(i))
	}
	fmt.Printf("memtable Put 1M: %.0f/s\n", 1000000/time.Since(start).Seconds())

	db, _ := lsm.Open(lsm.Options{
		Dir:            dir + "/lsm-test",
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	kvs := make([]lsm.KeyValue, 1000)
	for i := range kvs {
		kvs[i] = lsm.KeyValue{Key: fmt.Sprintf("key-%08d", i), Value: fmt.Sprintf("val-%08d", i)}
	}
	start = time.Now()
	for i := 0; i < 10000; i++ {
		db.PutBatch(kvs)
	}
	fmt.Printf("LSM PutBatch(1000) x10000: %.0f/s (%.0f writes/s)\n",
		10000/time.Since(start).Seconds(),
		10000*1000/time.Since(start).Seconds())
	db.Close()
}
