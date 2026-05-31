package lsm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aasheesh/lsmtree/internal/compaction"
	"github.com/aasheesh/lsmtree/internal/memtable"
	"github.com/aasheesh/lsmtree/internal/sstable"
	"github.com/aasheesh/lsmtree/internal/wal"
)

type Stats struct {
	WALBytesWritten        int64
	SSTableBytesWritten    int64
	CompactionBytesWritten int64
	FilesOpened            int64
	BloomFilterSkips       int64
	LevelSizes             []int64
	NumLevels              int
}

type LSM struct {
	dir      string
	memtable *memtable.MemTable
	wal      *wal.WAL
	mu       sync.RWMutex

	flushThreshold int
	maxL0Files     int
	baseLevelSize  int64
	numLevels      int

	levels [][]*sstable.SSTable

	stats Stats

	closed atomic.Bool
	nextID int64

	compactCh     chan struct{}
	compactCtx    context.Context
	compactCancel context.CancelFunc
	compactWg     sync.WaitGroup

	walBuf []byte
}

type Options struct {
	Dir            string
	FlushThreshold int
	NumLevels      int
	WALSyncMode    wal.SyncMode
	NoWAL          bool
}

func Open(opts Options) (*LSM, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	threshold := opts.FlushThreshold
	if threshold <= 0 {
		threshold = 4 << 20
	}
	numLevels := opts.NumLevels
	if numLevels <= 0 {
		numLevels = 7
	}

	compactCtx, compactCancel := context.WithCancel(context.Background())
	lsm := &LSM{
		dir:            opts.Dir,
		memtable:       memtable.New(),
		flushThreshold: threshold,
		maxL0Files:     8,
		baseLevelSize:  1 << 20,
		numLevels:      numLevels,
		levels:         make([][]*sstable.SSTable, numLevels),
		nextID:         time.Now().UnixNano(),
		compactCh:      make(chan struct{}, 1),
		compactCtx:     compactCtx,
		compactCancel:  compactCancel,
	}
	lsm.stats.LevelSizes = make([]int64, numLevels)

	if !opts.NoWAL {
		walPath := filepath.Join(opts.Dir, "wal.log")
		w, err := wal.New(walPath, opts.WALSyncMode)
		if err != nil {
			return nil, fmt.Errorf("wal: %w", err)
		}
		lsm.wal = w
		if err := lsm.recoverWAL(); err != nil {
			return nil, fmt.Errorf("wal recovery: %w", err)
		}
	}

	lsm.loadExistingSSTables()
	lsm.updateLevelStats()

	lsm.writeManifest()

	lsm.compactWg.Add(1)
	go lsm.compactionLoop()

	return lsm, nil
}

func manifestPath(dir string) string {
	return filepath.Join(dir, "MANIFEST")
}

func (lsm *LSM) writeManifest() error {
	snapshot := make([][]string, lsm.numLevels)
	for level := range lsm.levels {
		for _, sst := range lsm.levels[level] {
			snapshot[level] = append(snapshot[level], filepath.Base(sst.Path()))
		}
	}
	data, err := json.Marshal(map[string][][]string{"levels": snapshot})
	if err != nil {
		return err
	}
	tmpPath := manifestPath(lsm.dir) + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	f.Sync()
	f.Close()

	if err := os.Rename(tmpPath, manifestPath(lsm.dir)); err != nil {
		return err
	}

	dir, err := os.Open(lsm.dir)
	if err != nil {
		return err
	}
	dir.Sync()
	dir.Close()
	return nil
}

func (lsm *LSM) readManifest() ([][]string, error) {
	data, err := os.ReadFile(manifestPath(lsm.dir))
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Levels [][]string `json:"levels"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	return parsed.Levels, nil
}

func (lsm *LSM) recoverOrphans(known map[string]bool) {
	entries, err := os.ReadDir(lsm.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		if !known[e.Name()] {
			os.Remove(filepath.Join(lsm.dir, e.Name()))
		}
	}
}

func (lsm *LSM) loadExistingSSTables() {

	levelFiles, err := lsm.readManifest()
	if err == nil && len(levelFiles) == lsm.numLevels {
		known := map[string]bool{}
		for level := range levelFiles {
			for _, name := range levelFiles[level] {
				known[name] = true
				path := filepath.Join(lsm.dir, name)
				sst, err := sstable.Open(path)
				if err != nil {
					continue
				}
				lsm.levels[level] = append(lsm.levels[level], sst)
			}
		}
		lsm.recoverOrphans(known)
		return
	}

	entries, err := os.ReadDir(lsm.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		parts := strings.SplitN(e.Name(), "-", 2)
		if len(parts) != 2 {
			continue
		}
		level, err := strconv.Atoi(parts[0])
		if err != nil || level < 0 || level >= lsm.numLevels {
			continue
		}
		path := filepath.Join(lsm.dir, e.Name())
		sst, err := sstable.Open(path)
		if err != nil {
			continue
		}
		lsm.levels[level] = append(lsm.levels[level], sst)
	}
}

func (lsm *LSM) recoverWAL() error {
	entries, err := lsm.wal.Replay()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Tombstone {
			lsm.memtable.Delete(e.Key, e.Timestamp)
		} else {
			lsm.memtable.Put(e.Key, e.Value, e.Timestamp)
		}
	}
	if len(entries) > 0 {
		if err := lsm.wal.Reset(); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) Put(key, value string) error {
	return lsm.PutBatch([]KeyValue{{Key: key, Value: value}})
}

type KeyValue struct {
	Key   string
	Value string
}

func (lsm *LSM) PutBatch(kvs []KeyValue) error {
	if len(kvs) == 0 {
		return nil
	}

	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	ts := time.Now().UnixNano()

	if lsm.wal == nil {

		for i, kv := range kvs {
			lsm.memtable.Put(kv.Key, kv.Value, ts+int64(i))
		}
	} else {

		totalEstimate := 0
		for _, kv := range kvs {
			totalEstimate += len(kv.Key) + len(kv.Value) + 16
		}
		if cap(lsm.walBuf) < totalEstimate+8 {
			lsm.walBuf = make([]byte, 0, totalEstimate+8)
		}
		walBuf := lsm.walBuf[:0]
		var walTmp [10]byte

		walBuf = binary.AppendUvarint(walBuf, uint64(len(kvs)))
		crcOff := len(walBuf)
		walBuf = append(walBuf, 0, 0, 0, 0)

		for i, kv := range kvs {
			key := kv.Key
			val := kv.Value
			t := ts + int64(i)

			nn := binary.PutUvarint(walTmp[:], uint64(len(key)))
			walBuf = append(walBuf, walTmp[:nn]...)
			walBuf = append(walBuf, key...)
			nn = binary.PutUvarint(walTmp[:], uint64(len(val)))
			walBuf = append(walBuf, walTmp[:nn]...)
			walBuf = append(walBuf, val...)
			nn = binary.PutVarint(walTmp[:], t)
			walBuf = append(walBuf, walTmp[:nn]...)
			walBuf = append(walBuf, 0)

			lsm.memtable.Put(key, val, t)
		}

		crc := crc32.ChecksumIEEE(walBuf[crcOff+4:])
		binary.LittleEndian.PutUint32(walBuf[crcOff:], crc)

		if err := lsm.wal.AppendRaw(walBuf); err != nil {
			return err
		}
		atomic.AddInt64(&lsm.stats.WALBytesWritten, int64(totalEstimate))
	}

	if lsm.memtable.Size() >= lsm.flushThreshold {
		if err := lsm.flushMemtable(); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) flushMemtable() error {
	if lsm.memtable.Count() == 0 {
		return nil
	}

	entries := lsm.memtable.Entries()
	entriesCopy := make([]memtable.Entry, len(entries))
	copy(entriesCopy, entries)

	sstPath := filepath.Join(lsm.dir, fmt.Sprintf("0-%d.sst", lsm.nextID))
	lsm.nextID++

	w, err := sstable.CreateWriter(sstPath, len(entriesCopy), 128)
	if err != nil {
		return err
	}

	for _, e := range entriesCopy {
		if err := w.Append(e); err != nil {
			w.Close()
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}

	atomic.AddInt64(&lsm.stats.SSTableBytesWritten, w.Info())

	lsm.memtable = memtable.New()
	if lsm.wal != nil {
		if err := lsm.wal.Reset(); err != nil {
			os.Remove(sstPath)
			return err
		}
	}

	sst, err := sstable.OpenNoVerify(sstPath)
	if err != nil {
		return err
	}

	lsm.levels[0] = append(lsm.levels[0], sst)
	lsm.stats.LevelSizes[0] += sst.DataSize()

	if err := lsm.writeManifest(); err != nil {
		return err
	}

	select {
	case lsm.compactCh <- struct{}{}:
	default:
	}

	if len(lsm.levels[0]) > lsm.maxL0Files*2 {
		return lsm.compactL0ToL1()
	}

	return nil
}

func (lsm *LSM) compactL0ToL1() error {
	l0Files := lsm.levels[0]
	if len(l0Files) == 0 {
		return nil
	}

	sorted := make([]*sstable.SSTable, len(l0Files))
	copy(sorted, l0Files)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Path() < sorted[j].Path()
	})

	n := len(sorted) / 2
	if n < 1 {
		n = 1
	}
	if n > lsm.maxL0Files/2 {
		n = lsm.maxL0Files / 2
	}
	toCompact := sorted[:n]
	remaining := sorted[n:]

	minK := toCompact[0].MinKey()
	maxK := toCompact[0].MaxKey()
	for _, sst := range toCompact[1:] {
		if sst.MinKey() < minK {
			minK = sst.MinKey()
		}
		if sst.MaxKey() > maxK {
			maxK = sst.MaxKey()
		}
	}

	var overlappingL1 []*sstable.SSTable
	var nonOverlappingL1 []*sstable.SSTable
	for _, sst := range lsm.levels[1] {
		if sst.MaxKey() < minK || sst.MinKey() > maxK {
			nonOverlappingL1 = append(nonOverlappingL1, sst)
		} else {
			overlappingL1 = append(overlappingL1, sst)
		}
	}

	var inputs []*sstable.SSTable
	inputs = append(inputs, toCompact...)
	inputs = append(inputs, overlappingL1...)

	outputPaths, cstats, err := compaction.DoCompact(inputs, lsm.dir, 1, int(lsm.nextID))
	if err != nil {
		return fmt.Errorf("l0->l1 compact: %w", err)
	}

	if cstats != nil {
		atomic.AddInt64(&lsm.stats.CompactionBytesWritten, cstats.BytesWritten)
		lsm.nextID += int64(cstats.OutputFiles)
	}

	for _, sst := range toCompact {
		sst.Close()
		os.Remove(sst.Path())
		lsm.stats.LevelSizes[0] -= sst.DataSize()
	}

	for _, sst := range overlappingL1 {
		sst.Close()
		os.Remove(sst.Path())
		lsm.stats.LevelSizes[1] -= sst.DataSize()
	}

	lsm.levels[0] = remaining

	lsm.levels[1] = nonOverlappingL1
	for _, path := range outputPaths {
		sst, err := sstable.OpenNoVerify(path)
		if err != nil {
			return err
		}
		lsm.levels[1] = append(lsm.levels[1], sst)
		lsm.stats.LevelSizes[1] += sst.DataSize()
	}

	if err := lsm.writeManifest(); err != nil {
		return err
	}

	return nil
}

func (lsm *LSM) compactLevel(level int) error {
	if level < 1 || level >= lsm.numLevels-1 {
		return nil
	}

	files := lsm.levels[level]
	if len(files) == 0 {
		return nil
	}

	nextLevel := level + 1
	nextFiles := lsm.levels[nextLevel]

	var inputs []*sstable.SSTable
	inputs = append(inputs, files...)
	inputs = append(inputs, nextFiles...)

	outputPaths, cstats, err := compaction.DoCompact(inputs, lsm.dir, nextLevel, int(lsm.nextID))
	if err != nil {
		return fmt.Errorf("level %d compact: %w", level, err)
	}

	if cstats != nil {
		atomic.AddInt64(&lsm.stats.CompactionBytesWritten, cstats.BytesWritten)
		lsm.nextID += int64(cstats.OutputFiles)
	}

	for _, sst := range files {
		sst.Close()
		os.Remove(sst.Path())
		lsm.stats.LevelSizes[level] -= sst.DataSize()
	}
	lsm.levels[level] = nil

	for _, sst := range nextFiles {
		sst.Close()
		os.Remove(sst.Path())
		lsm.stats.LevelSizes[nextLevel] -= sst.DataSize()
	}
	lsm.levels[nextLevel] = nil

	for _, path := range outputPaths {
		sst, err := sstable.OpenNoVerify(path)
		if err != nil {
			return err
		}
		lsm.levels[nextLevel] = append(lsm.levels[nextLevel], sst)
		lsm.stats.LevelSizes[nextLevel] += sst.DataSize()
	}

	if err := lsm.writeManifest(); err != nil {
		return err
	}

	nextLimit := compaction.LevelSize(lsm.baseLevelSize, nextLevel)
	if lsm.stats.LevelSizes[nextLevel] > nextLimit && nextLevel < lsm.numLevels-1 {
		return lsm.compactLevel(nextLevel)
	}

	return nil
}

func (lsm *LSM) Get(key string) (string, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	if e, ok := lsm.memtable.Get(key); ok {
		if e.Tombstone {
			return "", false
		}
		return e.Value, true
	}

	for levelIdx := 0; levelIdx < lsm.numLevels; levelIdx++ {
		level := lsm.levels[levelIdx]
		if len(level) == 0 {
			continue
		}

		for _, sst := range level {
			if !sst.MayContain(key) {
				atomic.AddInt64(&lsm.stats.BloomFilterSkips, 1)
				continue
			}
			atomic.AddInt64(&lsm.stats.FilesOpened, 1)
			e, ok := sst.Get(key)
			if ok {
				if e.Tombstone {
					return "", false
				}
				return e.Value, true
			}
		}
	}

	return "", false
}

func (lsm *LSM) Delete(key string) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	ts := time.Now().UnixNano()

	if lsm.wal != nil {
		if err := lsm.wal.Append(memtable.Entry{Key: key, Timestamp: ts, Tombstone: true}); err != nil {
			return err
		}
		atomic.AddInt64(&lsm.stats.WALBytesWritten, int64(len(key)+16))
	}

	lsm.memtable.Delete(key, ts)
	return nil
}

func (lsm *LSM) updateLevelStats() {
	for i := range lsm.levels {
		lsm.stats.LevelSizes[i] = 0
		for _, sst := range lsm.levels[i] {
			lsm.stats.LevelSizes[i] += sst.DataSize()
		}
	}
}

func (lsm *LSM) Stats() Stats {
	s := Stats{
		WALBytesWritten:        atomic.LoadInt64(&lsm.stats.WALBytesWritten),
		SSTableBytesWritten:    atomic.LoadInt64(&lsm.stats.SSTableBytesWritten),
		CompactionBytesWritten: atomic.LoadInt64(&lsm.stats.CompactionBytesWritten),
		FilesOpened:            atomic.LoadInt64(&lsm.stats.FilesOpened),
		BloomFilterSkips:       atomic.LoadInt64(&lsm.stats.BloomFilterSkips),
	}
	lsm.mu.RLock()
	s.LevelSizes = make([]int64, lsm.numLevels)
	s.NumLevels = lsm.numLevels
	for i := range lsm.levels {
		sz := int64(0)
		for _, sst := range lsm.levels[i] {
			sz += sst.DataSize()
		}
		s.LevelSizes[i] = sz
	}
	lsm.mu.RUnlock()
	return s
}

func (lsm *LSM) NumLevels() int {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	return lsm.numLevels
}

func (lsm *LSM) FilesPerLevel() []int {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	result := make([]int, lsm.numLevels)
	for i := range lsm.levels {
		result[i] = len(lsm.levels[i])
	}
	return result
}

func (lsm *LSM) Close() error {
	lsm.closed.Store(true)
	lsm.compactCancel()
	lsm.compactWg.Wait()
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	for _, level := range lsm.levels {
		for _, sst := range level {
			sst.Close()
		}
	}
	if lsm.wal != nil {
		return lsm.wal.Close()
	}
	return nil
}

func (lsm *LSM) HasOverlappingLevels() bool {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	for level := 1; level < lsm.numLevels; level++ {
		files := lsm.levels[level]
		if len(files) <= 1 {
			continue
		}

		sorted := make([]*sstable.SSTable, len(files))
		copy(sorted, files)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].SparseIndex().Entries()[0].Key < sorted[j].SparseIndex().Entries()[0].Key
		})
		for i := 1; i < len(sorted); i++ {
			prevLast := sorted[i-1].SparseIndex().Entries()[sorted[i-1].SparseIndex().Count()-1].Key
			currFirst := sorted[i].SparseIndex().Entries()[0].Key
			if prevLast >= currFirst {
				return true
			}
		}
	}
	return false
}

func (lsm *LSM) TotalDataSize() int64 {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	total := lsm.memtable.Size()
	for i := range lsm.levels {
		for _, sst := range lsm.levels[i] {
			total += int(sst.DataSize())
		}
	}
	return int64(total)
}

func (lsm *LSM) compactionLoop() {
	defer lsm.compactWg.Done()
	for {
		select {
		case <-lsm.compactCtx.Done():
			return
		case <-lsm.compactCh:
			lsm.mu.Lock()
			if len(lsm.levels[0]) > lsm.maxL0Files {
				lsm.compactL0ToL1()
			}

			for level := 1; level < lsm.numLevels-1; level++ {
				limit := compaction.LevelSize(lsm.baseLevelSize, level)
				if lsm.stats.LevelSizes[level] > limit {
					lsm.compactLevel(level)
				}
			}
			lsm.mu.Unlock()
		}
	}
}

func WriteAmp(stats Stats) float64 {
	totalWritten := stats.SSTableBytesWritten + stats.CompactionBytesWritten
	ingested := stats.WALBytesWritten
	if ingested == 0 {
		return 0
	}
	return float64(totalWritten) / float64(ingested)
}
