package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/aasheesh/lsmtree/internal/lsm"
	"github.com/aasheesh/lsmtree/internal/wal"
)

func main() {
	dir, _ := os.MkdirTemp("", "lsm-cpuprof")

	db, _ := lsm.Open(lsm.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		WALSyncMode:    wal.SyncAsync,
	})

	N := 1000000

	f, _ := os.Create("/tmp/cpu.prof")
	pprof.StartCPUProfile(f)

	tf, _ := os.Create("/tmp/trace.out")
	trace.Start(tf)

	start := time.Now()
	batch := make([]lsm.KeyValue, 0, 1000)
	for i := 0; i < N; i++ {
		batch = append(batch, lsm.KeyValue{
			Key: fmt.Sprintf("key-%08d", i), Value: fmt.Sprintf("val-%08d", i),
		})
		if len(batch) >= 1000 {
			db.PutBatch(batch)
			batch = batch[:0]
		}
	}
	elapsed := time.Since(start)

	trace.Stop()
	tf.Close()
	pprof.StopCPUProfile()
	f.Close()

	writesPerSec := float64(N) / elapsed.Seconds()
	fmt.Printf("\n%d writes in %v = %.0f writes/s\n", N, elapsed, writesPerSec)
	stats := db.Stats()
	totalIO := stats.SSTableBytesWritten + stats.CompactionBytesWritten
	fmt.Printf("stats: flushes=%d ss=%dMB comp=%dMB total=%dMB wa=%.2f\n",
		stats.FilesOpened, stats.SSTableBytesWritten>>20,
		stats.CompactionBytesWritten>>20, totalIO>>20, lsm.WriteAmp(stats))
	fmt.Printf("levels: ")
	for i, sz := range stats.LevelSizes {
		if sz > 0 {
			fmt.Printf("L%d=%dMB ", i, sz>>20)
		}
	}
	fmt.Println()

	db.Close()
	os.RemoveAll(dir)

	fmt.Println("\nProfile saved to /tmp/cpu.prof")
	fmt.Println("Trace saved to /tmp/trace.out")
	fmt.Println("\nAnalyze with:")
	fmt.Println("  go tool pprof -http=:8080 cpu.prof")
	fmt.Println("  go tool trace trace.out")

	if runtime.GOOS == "linux" {
		fmt.Println("\nOr for perf (Linux):")
		fmt.Println("  perf record -F 997 -g ./profile")
		fmt.Println("  perf report -g graph")
	}
}
