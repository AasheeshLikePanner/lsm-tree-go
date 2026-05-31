package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aasheesh/lsmtree/internal/memtable"
)

const walVersion byte = 0x01

const initialMmapSize = 64 << 20

type SyncMode int

const (
	SyncAlways SyncMode = iota
	SyncPeriodic
	SyncAsync
	SyncIouring
)

type WAL struct {
	file    *os.File
	path    string
	mmap    []byte
	writeAt int64

	mu      sync.Mutex
	mode    SyncMode
	pending int

	asyncCh   chan struct{}
	asyncDone chan struct{}
	closed    atomic.Bool
	flushErr  atomic.Value

	ring *iouring
}

func New(path string, mode SyncMode) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	if stat.Size() == 0 {
		if _, err := f.Write([]byte{walVersion}); err != nil {
			f.Close()
			return nil, err
		}
	} else if mode == SyncIouring {
		f.Close()
		return nil, fmt.Errorf("io_uring: existing WAL must be recreated")
	}

	stat, _ = f.Stat()
	fileSize := stat.Size()

	mmapLen := fileSize
	if fileSize <= 1 {
		mmapLen = initialMmapSize
	}
	if err := f.Truncate(mmapLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: truncate %s: %w", path, err)
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(mmapLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: mmap %s: %w", path, err)
	}

	w := &WAL{
		file:      f,
		path:      path,
		mmap:      mmap,
		writeAt:   fileSize,
		mode:      mode,
		asyncCh:   make(chan struct{}, 1),
		asyncDone: make(chan struct{}),
	}

	switch mode {
	case SyncAsync:
		go w.asyncWriter()
	case SyncIouring:
		ring, err := newIORing(int(f.Fd()))
		if err != nil {
			w.Close()
			return nil, fmt.Errorf("io_uring: %w", err)
		}
		w.ring = ring
	}

	return w, nil
}

func (w *WAL) Append(entry memtable.Entry) error {
	return w.AppendBatch([]memtable.Entry{entry})
}

func (w *WAL) AppendBatch(entries []memtable.Entry) error {
	buf := serializeEntries(entries)
	return w.AppendRaw(buf)
}

func (w *WAL) AppendRaw(buf []byte) error {
	switch w.mode {

	case SyncIouring:
		return w.ring.submitWrite(buf)

	case SyncAsync:
		w.mu.Lock()
		if err := w.writeToMmap(buf); err != nil {
			w.mu.Unlock()
			return err
		}
		w.mu.Unlock()
		select {
		case w.asyncCh <- struct{}{}:
		default:
		}
		if err, ok := w.flushErr.Load().(error); ok && err != nil {
			return err
		}
		return nil

	case SyncPeriodic:
		w.mu.Lock()
		if err := w.writeToMmap(buf); err != nil {
			w.mu.Unlock()
			return err
		}
		w.pending++
		doSync := w.pending >= 100
		if doSync {
			w.pending = 0
		}
		w.mu.Unlock()
		if doSync {
			return syncFile(w.file)
		}
		return nil

	default:
		w.mu.Lock()
		if err := w.writeToMmap(buf); err != nil {
			w.mu.Unlock()
			return err
		}
		w.mu.Unlock()
		return syncFile(w.file)
	}
}

func (w *WAL) writeToMmap(buf []byte) error {
	need := w.writeAt + int64(len(buf))
	if need > int64(len(w.mmap)) {

		newSize := len(w.mmap) * 2
		for newSize < int(need) {
			newSize *= 2
		}
		if err := syscall.Munmap(w.mmap); err != nil {
			return fmt.Errorf("wal: munmap grow: %w", err)
		}
		if err := w.file.Truncate(int64(newSize)); err != nil {
			return fmt.Errorf("wal: truncate grow: %w", err)
		}
		m, err := syscall.Mmap(int(w.file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return fmt.Errorf("wal: mmap grow: %w", err)
		}
		w.mmap = m
	}

	n := copy(w.mmap[w.writeAt:], buf)
	w.writeAt += int64(n)
	return nil
}

func (w *WAL) asyncWriter() {
	const flushInterval = 10 * time.Millisecond

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if err := syncFile(w.file); err != nil {
			w.flushErr.Store(err)
		}
	}

	dirty := false
	for {
		select {
		case _, ok := <-w.asyncCh:
			if !ok {
				if dirty {
					flush()
				}
				close(w.asyncDone)
				return
			}
			dirty = true
		case <-ticker.C:
			if dirty {
				flush()
				dirty = false
			}
		}
	}
}

func (w *WAL) Replay() ([]memtable.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	buf := w.mmap[:w.writeAt]
	if len(buf) > 0 && buf[0] == walVersion {
		buf = buf[1:]
	}

	var entries []memtable.Entry
	for len(buf) > 0 {

		numEntries, n := binary.Uvarint(buf)
		if n <= 0 {
			break
		}
		buf = buf[n:]
		if len(buf) < 4 {
			break
		}
		storedCRC := binary.LittleEndian.Uint32(buf[:4])
		buf = buf[4:]

		entryDataStart := buf

		batchOK := true
		batchEntries := make([]memtable.Entry, 0, numEntries)
		for i := uint64(0); i < numEntries; i++ {
			keyLen, n := binary.Uvarint(buf)
			if n <= 0 {
				batchOK = false
				break
			}
			buf = buf[n:]
			if len(buf) < int(keyLen) {
				batchOK = false
				break
			}
			key := string(buf[:keyLen])
			buf = buf[keyLen:]

			valLen, n := binary.Uvarint(buf)
			if n <= 0 {
				batchOK = false
				break
			}
			buf = buf[n:]
			if len(buf) < int(valLen) {
				batchOK = false
				break
			}
			value := string(buf[:valLen])
			buf = buf[valLen:]

			ts, n := binary.Varint(buf)
			if n <= 0 {
				batchOK = false
				break
			}
			buf = buf[n:]

			if len(buf) < 1 {
				batchOK = false
				break
			}
			tombstone := buf[0] == 1
			buf = buf[1:]

			batchEntries = append(batchEntries, memtable.Entry{
				Key:       key,
				Value:     value,
				Timestamp: ts,
				Tombstone: tombstone,
			})
		}

		if !batchOK {

			break
		}

		entryDataLen := len(entryDataStart) - len(buf)
		if crc32.ChecksumIEEE(entryDataStart[:entryDataLen]) != storedCRC {
			break
		}

		entries = append(entries, batchEntries...)
	}

	return entries, nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := syscall.Munmap(w.mmap); err != nil {
		return fmt.Errorf("wal: munmap reset: %w", err)
	}
	w.mmap = nil

	if w.mode == SyncIouring {
		w.ring.close()
	}
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate reset: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal: seek reset: %w", err)
	}

	if _, err := w.file.Write([]byte{walVersion}); err != nil {
		return err
	}
	if err := w.file.Truncate(initialMmapSize); err != nil {
		return fmt.Errorf("wal: truncate reset mmap: %w", err)
	}
	m, err := syscall.Mmap(int(w.file.Fd()), 0, initialMmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("wal: mmap reset: %w", err)
	}
	w.mmap = m
	w.writeAt = 1

	if w.mode == SyncIouring {
		ring, err := newIORing(int(w.file.Fd()))
		if err != nil {
			return fmt.Errorf("io_uring reset: %w", err)
		}
		w.ring = ring
	}
	return nil
}

func (w *WAL) Close() error {
	w.closed.Store(true)

	switch w.mode {
	case SyncAsync:
		close(w.asyncCh)
		<-w.asyncDone
		if err, ok := w.flushErr.Load().(error); ok && err != nil {
			return err
		}
	case SyncIouring:
		w.ring.flush()
		w.ring.close()
	}

	if w.mmap != nil {
		syscall.Munmap(w.mmap)
		w.mmap = nil
	}

	if err := w.file.Truncate(w.writeAt); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

func serializeEntries(entries []memtable.Entry) []byte {
	total := 0
	for _, e := range entries {
		base := len(e.Key) + len(e.Value) + 16
		total += base + uvarintSize(uint64(len(e.Key))) + uvarintSize(uint64(len(e.Value))) + varintSize(e.Timestamp) + 1
	}
	buf := make([]byte, 0, total+8)
	var tmp [10]byte

	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	crcOff := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	for _, e := range entries {
		var tomb byte
		if e.Tombstone {
			tomb = 1
		}
		n := binary.PutUvarint(tmp[:], uint64(len(e.Key)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, e.Key...)
		n = binary.PutUvarint(tmp[:], uint64(len(e.Value)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, e.Value...)
		n = binary.PutVarint(tmp[:], e.Timestamp)
		buf = append(buf, tmp[:n]...)
		buf = append(buf, tomb)
	}

	crc := crc32.ChecksumIEEE(buf[crcOff+4:])
	binary.LittleEndian.PutUint32(buf[crcOff:], crc)
	return buf
}

func uvarintSize(v uint64) int {
	if v == 0 {
		return 1
	}
	n := 0
	for v > 0 {
		v >>= 7
		n++
	}
	return n
}

func varintSize(v int64) int {
	uv := uint64(v) << 1
	if v < 0 {
		uv = ^uv
	}
	return uvarintSize(uv)
}
