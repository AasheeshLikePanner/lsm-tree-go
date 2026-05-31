package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/wal"
	badger "github.com/dgraph-io/badger/v4"
	bolt "go.etcd.io/bbolt"
)

const N = 1000000
const reads = 100000
const batchSize = 1000

type result struct {
	label  string
	seqWrt float64
	rndWrt float64
	read   float64
	missRd float64
	err    string
}

func main() {
	fmt.Printf("=== FAIR COMPARISON: LSM Tree vs BadgerDB vs BoltDB ===\n")
	fmt.Printf("Go %s | %d CPUs | %s/%s\n", runtime.Version(), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)
	fmt.Printf("All engines: N=%d, batch=%d, reads=%d, 11-byte keys & values\n\n", N, batchSize, reads)

	results := runAll()

	fmt.Println(string(repBytes('=', 100)))
	fmt.Printf("%-30s %14s %14s %14s %14s\n", "Engine", "SeqWrite", "RandWrite", "Read", "MissRead")
	fmt.Println(string(repBytes('-', 100)))
	for _, r := range results {
		if r.err != "" {
			fmt.Printf("%-30s %14s %s\n", r.label, "ERR", r.err)
			continue
		}
		fmt.Printf("%-30s %14.0f/s %14.0f/s %14.0f/s %14.0f/s\n",
			r.label, r.seqWrt, r.rndWrt, r.read, r.missRd)
	}

	fmt.Println()
	fmt.Println(string(repBytes('=', 100)))
	fmt.Println("DURABILITY NOTES")
	fmt.Println(string(repBytes('-', 100)))
	fmt.Println("LSM   NoWAL      : no WAL, no fsync; no crash recovery (ceiling perf)")
	fmt.Println("LSM   SyncAsync  : no fsync; up to ~10ms of writes lost on crash")
	fmt.Println("LSM   SyncAlways : fsync per 1k batch; each batch durable")
	fmt.Println("BadgerDB NoSync  : SyncWrites=false + WriteBatch; no fsync on flush")
	fmt.Println("BadgerDB Sync    : SyncWrites=true + WriteBatch; fsync per 1k flush")
	fmt.Println("BoltDB NoSync    : NoSync=true + batched txn (1k puts/txn); no fsync")
	fmt.Println("BoltDB Sync      : NoSync=false + batched txn; fsync per tx commit")
	fmt.Println()
	fmt.Println("BoltDB batched txn (1k puts per Update) = 1 fsync / 1k writes")
	fmt.Println("LSM PutBatch(1000) with SyncAlways = 1 fsync / 1k writes (matched)")
	fmt.Println("BadgerDB WriteBatch + flush every 1k + SyncWrites = 1 fsync / 1k writes (matched)")
}

func runAll() []result {
	fns := []struct {
		label string
		fn    func() result
	}{
		{"LSM NoWAL (ours)", benchLSMNoWAL},
		{"LSM SyncAsync (ours)", benchLSMAsync},
		{"BadgerDB WriteBatch", benchBadgerAsync},
		{"BoltDB batched txn", benchBoltAsync},
		{"LSM SyncAlways (ours)", benchLSMSync},
		{"BadgerDB SyncWrites", benchBadgerSync},
		{"BoltDB fsync/commit", benchBoltSync},
	}
	results := make([]result, len(fns))
	for i, f := range fns {
		r := f.fn()
		r.label = f.label
		results[i] = r
	}
	return results
}

func benchLSMAsync() result {
	dir, _ := os.MkdirTemp("", "lsm-async")
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAsync,
	})
	if err != nil {
		return result{err: err.Error()}
	}
	defer db.Close()

	os.Stderr.WriteString("  LSM SyncAsync: 1M keys + 100k reads...\n")

	seq := runLSMSeq(db)
	rnd := runLSMRnd(db)
	rd := runLSMRead(db)
	ms := runLSMMiss(db)

	return result{seqWrt: seq, rndWrt: rnd, read: rd, missRd: ms}
}

func benchLSMNoWAL() result {
	dir, _ := os.MkdirTemp("", "lsm-nowal")
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		NoWAL:          true,
	})
	if err != nil {
		return result{err: err.Error()}
	}
	defer db.Close()

	os.Stderr.WriteString("  LSM NoWAL: 1M keys + 100k reads...\n")

	seq := runLSMSeq(db)
	rnd := runLSMRnd(db)
	rd := runLSMRead(db)
	ms := runLSMMiss(db)

	return result{seqWrt: seq, rndWrt: rnd, read: rd, missRd: ms}
}

func benchLSMSync() result {
	dir, _ := os.MkdirTemp("", "lsm-sync")
	defer os.RemoveAll(dir)

	db, err := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 4 << 20,
		WALSyncMode:    wal.SyncAlways,
	})
	if err != nil {
		return result{err: err.Error()}
	}
	defer db.Close()

	os.Stderr.WriteString("  LSM SyncAlways: 1M keys (fsync/batch)...\n")

	seq := runLSMSeq(db)
	rnd := runLSMRnd(db)
	rd := runLSMRead(db)
	ms := runLSMMiss(db)

	return result{seqWrt: seq, rndWrt: rnd, read: rd, missRd: ms}
}

func runLSMSeq(db *lsm.LSM) float64 {
	batch := make([]lsm.KeyValue, 0, batchSize)
	start := time.Now()
	for i := 0; i < N; i++ {
		batch = append(batch, lsm.KeyValue{
			Key: fmt.Sprintf("key-%08d", i), Value: fmt.Sprintf("val-%08d", i),
		})
		if len(batch) >= batchSize {
			db.PutBatch(batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		db.PutBatch(batch)
	}
	return float64(N) / time.Since(start).Seconds()
}

func runLSMRnd(db *lsm.LSM) float64 {
	batch := make([]lsm.KeyValue, 0, batchSize)
	start := time.Now()
	for i := 0; i < N; i++ {
		batch = append(batch, lsm.KeyValue{
			Key: fmt.Sprintf("key-%08d", rand.Intn(N)), Value: fmt.Sprintf("val-%08d", i+N),
		})
		if len(batch) >= batchSize {
			db.PutBatch(batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		db.PutBatch(batch)
	}
	return float64(N) / time.Since(start).Seconds()
}

func runLSMRead(db *lsm.LSM) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.Get(fmt.Sprintf("key-%08d", rand.Intn(N)))
	}
	return float64(reads) / time.Since(start).Seconds()
}

func runLSMMiss(db *lsm.LSM) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.Get(fmt.Sprintf("missing-%08d", rand.Intn(1<<30)))
	}
	return float64(reads) / time.Since(start).Seconds()
}

func benchBadgerAsync() result {
	return runBadger(false)
}

func benchBadgerSync() result {
	return runBadger(true)
}

func runBadger(syncWrites bool) result {
	dir, _ := os.MkdirTemp("", "badger-fair")
	defer os.RemoveAll(dir)

	opts := badger.DefaultOptions(dir).WithLogger(nil).WithSyncWrites(syncWrites)
	db, err := badger.Open(opts)
	if err != nil {
		return result{err: err.Error()}
	}
	defer db.Close()

	mode := "NoSync"
	if syncWrites {
		mode = "Sync"
	}
	os.Stderr.WriteString(fmt.Sprintf("  BadgerDB %s: 1M keys (WriteBatch + 1k flush)...\n", mode))

	seq := runBadgerSeq(db)
	rnd := runBadgerRnd(db)
	rd := runBadgerRead(db)
	ms := runBadgerMiss(db)

	return result{seqWrt: seq, rndWrt: rnd, read: rd, missRd: ms}
}

func runBadgerSeq(db *badger.DB) float64 {
	start := time.Now()
	wb := db.NewWriteBatch()
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%08d", i)
		v := fmt.Sprintf("val-%08d", i)
		wb.Set([]byte(k), []byte(v))
		if (i+1)%batchSize == 0 {
			wb.Flush()
		}
	}
	wb.Flush()
	wb.Cancel()
	return float64(N) / time.Since(start).Seconds()
}

func runBadgerRnd(db *badger.DB) float64 {
	start := time.Now()
	wb := db.NewWriteBatch()
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%08d", rand.Intn(N))
		v := fmt.Sprintf("val-%08d", i+N)
		wb.Set([]byte(k), []byte(v))
		if (i+1)%batchSize == 0 {
			wb.Flush()
		}
	}
	wb.Flush()
	wb.Cancel()
	return float64(N) / time.Since(start).Seconds()
}

func runBadgerRead(db *badger.DB) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.View(func(txn *badger.Txn) error {
			txn.Get([]byte(fmt.Sprintf("key-%08d", rand.Intn(N))))
			return nil
		})
	}
	return float64(reads) / time.Since(start).Seconds()
}

func runBadgerMiss(db *badger.DB) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.View(func(txn *badger.Txn) error {
			txn.Get([]byte(fmt.Sprintf("missing-%08d", rand.Intn(1<<30))))
			return nil
		})
	}
	return float64(reads) / time.Since(start).Seconds()
}

func benchBoltAsync() result {
	return runBolt(true)
}

func benchBoltSync() result {
	return runBolt(false)
}

func runBolt(noSync bool) result {
	dir, _ := os.MkdirTemp("", "bolt-fair")
	defer os.RemoveAll(dir)

	opts := *bolt.DefaultOptions
	opts.NoSync = noSync
	db, err := bolt.Open(dir+"/bolt.db", 0600, &opts)
	if err != nil {
		return result{err: err.Error()}
	}
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("default"))
		return err
	})

	mode := "NoSync"
	if !noSync {
		mode = "Sync"
	}
	os.Stderr.WriteString(fmt.Sprintf("  BoltDB %s: 1M keys (batched txn + 1k puts/txn)...\n", mode))

	seq := runBoltSeq(db)
	rnd := runBoltRnd(db)
	rd := runBoltRead(db)
	ms := runBoltMiss(db)

	return result{seqWrt: seq, rndWrt: rnd, read: rd, missRd: ms}
}

func runBoltSeq(db *bolt.DB) float64 {
	start := time.Now()
	for i := 0; i < N; i += batchSize {
		end := i + batchSize
		if end > N {
			end = N
		}
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("default"))
			for j := i; j < end; j++ {
				if err := b.Put([]byte(fmt.Sprintf("key-%08d", j)), []byte(fmt.Sprintf("val-%08d", j))); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return float64(N) / time.Since(start).Seconds()
}

func runBoltRnd(db *bolt.DB) float64 {
	start := time.Now()
	for i := 0; i < N; i += batchSize {
		end := i + batchSize
		if end > N {
			end = N
		}
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("default"))
			for j := i; j < end; j++ {
				if err := b.Put([]byte(fmt.Sprintf("key-%08d", rand.Intn(N))), []byte(fmt.Sprintf("val-%08d", j+N))); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return float64(N) / time.Since(start).Seconds()
}

func runBoltRead(db *bolt.DB) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.View(func(tx *bolt.Tx) error {
			tx.Bucket([]byte("default")).Get([]byte(fmt.Sprintf("key-%08d", rand.Intn(N))))
			return nil
		})
	}
	return float64(reads) / time.Since(start).Seconds()
}

func runBoltMiss(db *bolt.DB) float64 {
	start := time.Now()
	for i := 0; i < reads; i++ {
		db.View(func(tx *bolt.Tx) error {
			tx.Bucket([]byte("default")).Get([]byte(fmt.Sprintf("missing-%08d", rand.Intn(1<<30))))
			return nil
		})
	}
	return float64(reads) / time.Since(start).Seconds()
}

func repBytes(b byte, n int) []byte {
	r := make([]byte, n)
	for i := range r {
		r[i] = b
	}
	return r
}
