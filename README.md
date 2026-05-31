# lsm-tree-go

An LSM tree storage engine in Go — the data structure inside RocksDB, Cassandra, TiKV, and CockroachDB — built to be readable and benchmarked honestly against BadgerDB and BoltDB.

5x faster than BoltDB on random writes. Profiled from 250k to ~2M writes/sec.

## The idea

B-trees keep data sorted on disk at all times, so every insert is a random write. Under write-heavy load, random writes are the bottleneck — BoltDB drops from 2.77M sequential writes/sec to 234k random.

An LSM tree never writes randomly. Writes go to an in-memory MemTable first. When it fills, the whole thing flushes to disk in one sequential write as an immutable SSTable. Old SSTables merge together in the background (compaction). The cost is read complexity — data is spread across many files — which bloom filters, sparse indexes, and levels manage.

## Benchmarks

Linux, 4 CPU, 1M keys, 11-byte keys+values, batch=1000:

| Engine | SeqWrite | RandWrite | Read | MissRead |
|--------|----------|-----------|------|----------|
| BadgerDB | 7.30M/s | 6.97M/s | 1.29M/s | 1.49M/s |
| BoltDB | 2.77M/s | 0.23M/s | 1.17M/s | 1.96M/s |
| **lsm-tree-go** | **1.98M/s** | **1.19M/s** | **0.44M/s** | **1.08M/s** |

5x faster than BoltDB on random writes — the workload LSM trees are built for. 4x behind BadgerDB, which uses a value-log (WiscKey) design that stores values separately and keeps only keys in the tree, moving far less data during compaction.

## Architecture

```
WRITE PATH
  Put(key, value)
    → append to write-ahead log (mmap, zero syscall)
    → insert into MemTable
    → on full: flush MemTable → immutable SSTable
    → background: compact L0 → L1 → L2

READ PATH
  Get(key)
    → check MemTable
    → check L0 SSTables (bloom filter skips files)
    → check L1+ SSTables (one file per level via sparse index)
```

## Components

| Component | Description |
|-----------|-------------|
| MemTable | In-memory sorted map, pre-allocated capacity |
| WAL | mmap'd write-ahead log, 3 sync modes + NoWAL |
| SSTable | mmap'd immutable file: data + sparse index + bloom filter |
| Bloom filter | Double-hashing, 10 bits/key, ~0.84% false positive rate |
| Sparse index | Every 128th key → byte offset, binary search + scan |
| Compaction | Streaming k-way merge (heap), L0→L1 background goroutine |

## How it got fast

Every optimization came from profiling with pprof, not guessing. Each fix revealed the next bottleneck:

1. Write syscall (34% CPU) → mmap WAL → 2.16%
2. GC + sort in compaction (48%) → streaming k-way merge heap
3. SSTable read syscall → mmap reads → +234% read throughput
4. CRC read-back on flush → incremental CRC
5. MemTable map growth → capacity hint

Result: 250k → 1.98M sequential writes/sec.

## Correctness

All tests pass under `go test -race`:

- 1M keys write-read-verify, including overwrites (newest version wins)
- Crash recovery: SIGKILL mid-write, restart, replay WAL, zero loss
- Tombstone propagation through compaction to the bottom level

## Running

```bash
go run ./cmd/lsmdemo/      # demo
go run ./bench/compare/    # benchmark vs BadgerDB and BoltDB
go test -race ./...        # full test suite
```

## Limitations

- Single node, single writer
- Values stored inline (no value-log separation like BadgerDB)
- No transactions or snapshots
- Linux-optimized (fdatasync, mmap paths)

## License

MIT
