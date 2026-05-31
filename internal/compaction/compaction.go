package compaction

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/aasheesh/lsmtree/internal/memtable"
	"github.com/aasheesh/lsmtree/internal/sstable"
)

const LevelMultiplier = 10

type Stats struct {
	BytesWritten int64
	InputFiles   int
	OutputFiles  int
}

func LevelSize(baseSize int64, level int) int64 {
	return baseSize * int64(math.Pow(LevelMultiplier, float64(level)))
}

type heapEntry struct {
	entry memtable.Entry
	iter  *sstable.EntryIter
}

type mergeHeap []heapEntry

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].entry.Key != h[j].entry.Key {
		return h[i].entry.Key < h[j].entry.Key
	}
	return h[i].entry.Timestamp > h[j].entry.Timestamp
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(heapEntry)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func DoCompact(sstables []*sstable.SSTable, outputDir string, level int, id int) ([]string, *Stats, error) {
	if len(sstables) == 0 {
		return nil, nil, nil
	}

	isBottomLevel := level >= 6

	var h mergeHeap
	for _, sst := range sstables {
		it := sst.Iter()
		entry, ok := it.Next()
		if !ok {
			continue
		}
		h = append(h, heapEntry{entry: entry, iter: it})
	}
	heap.Init(&h)

	if len(h) == 0 {
		return nil, nil, nil
	}

	targetSize := int64(4 << 20)
	var outputPaths []string
	var stats Stats
	lastKey := ""

	var w *sstable.Writer
	var chunkSize int64
	fileID := id

	const entriesPerChunk = 100000

	flushChunk := func() error {
		if w == nil {
			return nil
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("close output: %w", err)
		}
		stats.BytesWritten += w.Info()
		stats.OutputFiles++
		w = nil
		chunkSize = 0
		return nil
	}

	for h.Len() > 0 {
		he := heap.Pop(&h).(heapEntry)
		e := he.entry

		if next, ok := he.iter.Next(); ok {
			heap.Push(&h, heapEntry{entry: next, iter: he.iter})
		}

		if e.Key == lastKey {
			continue
		}

		if e.Tombstone && isBottomLevel {
			lastKey = e.Key
			continue
		}

		es := int64(len(e.Key) + len(e.Value) + 16)

		if w != nil && chunkSize+es > targetSize {
			if err := flushChunk(); err != nil {
				return nil, nil, err
			}
		}

		if w == nil {
			outputPath := filepath.Join(outputDir, fmt.Sprintf("%d-%08d.sst", level, fileID))
			fileID++
			var err error
			w, err = sstable.CreateWriter(outputPath, entriesPerChunk, 128)
			if err != nil {
				return nil, nil, fmt.Errorf("create writer: %w", err)
			}
			outputPaths = append(outputPaths, outputPath)
		}

		if err := w.Append(e); err != nil {
			w.Close()
			return nil, nil, fmt.Errorf("append: %w", err)
		}
		chunkSize += es
		lastKey = e.Key
	}

	if w != nil {
		if err := flushChunk(); err != nil {
			return nil, nil, err
		}
	}

	stats.InputFiles = len(sstables)
	return outputPaths, &stats, nil
}

func CleanupSSTables(paths []string) {
	for _, p := range paths {
		os.Remove(p)
	}
}
