package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"

	"github.com/aasheesh/lsmtree/internal/bloom"
	"github.com/aasheesh/lsmtree/internal/memtable"
	"github.com/aasheesh/lsmtree/internal/sparseindex"
)

const initialMmapSize = 8 << 20

const (
	magicNumber = "LSM2"
	footerSize  = 24
)

type SSTable struct {
	path        string
	file        *os.File
	sparseIndex *sparseindex.SparseIndex
	bloomFilter *bloom.Filter
	data        []byte
	dataSize    int64
}

type Writer struct {
	file        *os.File
	mmap        []byte
	mmapCap     int64
	path        string
	count       int
	bytePos     int64
	sparseIndex *sparseindex.SparseIndex
	bloomFilter *bloom.Filter
	dataCRC     uint32
	scratch     []byte
}

func CreateWriter(path string, expectedEntries int, step int) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if step <= 0 {
		step = 128
	}
	si := sparseindex.New(step)
	bf := bloom.New(expectedEntries, 10)

	if err := f.Truncate(initialMmapSize); err != nil {
		f.Close()
		return nil, err
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, initialMmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Writer{
		file:        f,
		mmap:        mmap,
		mmapCap:     initialMmapSize,
		path:        path,
		sparseIndex: si,
		bloomFilter: bf,
	}, nil
}

func (w *Writer) grow(target int64) error {

	if err := syscall.Munmap(w.mmap); err != nil {
		return err
	}
	for w.mmapCap <= target {
		w.mmapCap *= 2
	}
	if err := w.file.Truncate(w.mmapCap); err != nil {
		return err
	}
	var err error
	w.mmap, err = syscall.Mmap(int(w.file.Fd()), 0, int(w.mmapCap), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return err
}

func (w *Writer) Append(entry memtable.Entry) error {
	var tomb byte
	if entry.Tombstone {
		tomb = 1
	}

	keyLen := len(entry.Key)
	valLen := len(entry.Value)

	w.scratch = w.scratch[:0]
	w.scratch = binary.AppendUvarint(w.scratch, uint64(keyLen))
	w.scratch = append(w.scratch, entry.Key...)
	w.scratch = binary.AppendUvarint(w.scratch, uint64(valLen))
	w.scratch = append(w.scratch, entry.Value...)
	w.scratch = binary.AppendVarint(w.scratch, entry.Timestamp)
	w.scratch = append(w.scratch, tomb)
	crc := crc32.ChecksumIEEE(w.scratch)
	w.scratch = binary.LittleEndian.AppendUint32(w.scratch, crc)

	need := w.bytePos + int64(len(w.scratch))
	if need > w.mmapCap {
		if err := w.grow(need); err != nil {
			return err
		}
	}

	copy(w.mmap[w.bytePos:], w.scratch)
	w.dataCRC = crc32.Update(w.dataCRC, crc32.IEEETable, w.scratch)
	w.bytePos += int64(len(w.scratch))
	w.count++

	w.bloomFilter.Add(entry.Key)

	if w.sparseIndex.ShouldIndex(w.count - 1) {
		w.sparseIndex.Add(entry.Key, w.bytePos-int64(len(w.scratch)))
	}

	return nil
}

func (w *Writer) Close() error {
	defer w.file.Close()
	defer syscall.Munmap(w.mmap)

	indexBuf := w.writeIndexBlock()
	bloomBuf := w.writeBloomBlock()

	indexOffset := w.bytePos
	copy(w.mmap[w.bytePos:], indexBuf)
	w.bytePos += int64(len(indexBuf))

	bloomOffset := w.bytePos
	copy(w.mmap[w.bytePos:], bloomBuf)
	w.bytePos += int64(len(bloomBuf))

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[16:20], w.dataCRC)
	copy(footer[20:24], magicNumber)
	copy(w.mmap[w.bytePos:], footer)
	w.bytePos += footerSize

	if err := w.file.Truncate(w.bytePos); err != nil {
		return err
	}
	return syncFd(int(w.file.Fd()))
}

func (w *Writer) Info() int64 {
	return w.bytePos
}

func (w *Writer) writeIndexBlock() []byte {
	entries := w.sparseIndex.Entries()
	buf := make([]byte, 0, len(entries)*32)
	buf = binary.AppendUvarint(buf, uint64(w.sparseIndex.Step()))
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, e := range entries {
		buf = binary.AppendUvarint(buf, uint64(len(e.Key)))
		buf = append(buf, e.Key...)
		buf = binary.AppendUvarint(buf, uint64(e.Offset))
	}
	return buf
}

func (w *Writer) writeBloomBlock() []byte {
	if w.bloomFilter == nil {
		var buf []byte
		buf = binary.AppendUvarint(buf, 0)
		return buf
	}
	return bloom.Serialize(w.bloomFilter)
}

func Open(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fileSize := int(stat.Size())
	if fileSize < footerSize {
		f.Close()
		return nil, errors.New("sstable: file too small")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: mmap %s: %w", path, err)
	}

	footerBuf := data[fileSize-footerSize:]

	if string(footerBuf[20:24]) != magicNumber {

		if string(footerBuf[16:20]) == "LSM1" {
			indexOffset := binary.LittleEndian.Uint64(footerBuf[0:8])
			bloomOffset := binary.LittleEndian.Uint64(footerBuf[8:16])
			syscall.Munmap(data)
			return openV1(f, path, stat, indexOffset, bloomOffset)
		}
		syscall.Munmap(data)
		f.Close()
		return nil, fmt.Errorf("sstable: invalid magic number")
	}

	indexOffset := binary.LittleEndian.Uint64(footerBuf[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footerBuf[8:16])
	dataCRC := binary.LittleEndian.Uint32(footerBuf[16:20])
	dataSize := int64(indexOffset)

	if crc32.ChecksumIEEE(data[:dataSize]) != dataCRC {
		syscall.Munmap(data)
		f.Close()
		return nil, fmt.Errorf("sstable: data CRC mismatch in %s", path)
	}

	indexBuf := data[indexOffset:bloomOffset]
	si := parseIndexBlock(indexBuf)

	bloomLen := uint64(fileSize) - bloomOffset - footerSize
	bf := parseBloomBlock(data[bloomOffset:], int64(bloomLen))

	s := &SSTable{
		path:        path,
		file:        f,
		sparseIndex: si,
		bloomFilter: bf,
		data:        data,
		dataSize:    dataSize,
	}

	return s, nil
}

func OpenNoVerify(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := int(stat.Size())
	if fileSize < footerSize {
		f.Close()
		return nil, errors.New("sstable: file too small")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: mmap %s: %w", path, err)
	}

	footerBuf := data[fileSize-footerSize:]

	var indexOffset, bloomOffset uint64
	if string(footerBuf[20:24]) == magicNumber {
		indexOffset = binary.LittleEndian.Uint64(footerBuf[0:8])
		bloomOffset = binary.LittleEndian.Uint64(footerBuf[8:16])
	} else if string(footerBuf[16:20]) == "LSM1" {
		indexOffset = binary.LittleEndian.Uint64(footerBuf[0:8])
		bloomOffset = binary.LittleEndian.Uint64(footerBuf[8:16])
		syscall.Munmap(data)
		return openV1(f, path, stat, indexOffset, bloomOffset)
	} else {
		syscall.Munmap(data)
		f.Close()
		return nil, fmt.Errorf("sstable: invalid magic number")
	}

	indexBuf := data[indexOffset:bloomOffset]
	si := parseIndexBlock(indexBuf)

	bloomLen := uint64(fileSize) - bloomOffset - footerSize
	bf := parseBloomBlock(data[bloomOffset:], int64(bloomLen))

	s := &SSTable{
		path:        path,
		file:        f,
		sparseIndex: si,
		bloomFilter: bf,
		data:        data,
		dataSize:    int64(indexOffset),
	}
	return s, nil
}

func openV1(f *os.File, path string, stat os.FileInfo, indexOffset, bloomOffset uint64) (*SSTable, error) {
	fileSize := int(stat.Size())
	data, err := syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: mmap %s: %w", path, err)
	}

	indexBuf := data[indexOffset:bloomOffset]
	si := parseIndexBlock(indexBuf)

	bloomLen := uint64(fileSize) - bloomOffset - 20
	bf := parseBloomBlock(data[bloomOffset:], int64(bloomLen))

	s := &SSTable{
		path:        path,
		file:        f,
		sparseIndex: si,
		bloomFilter: bf,
		data:        data,
		dataSize:    int64(indexOffset),
	}
	return s, nil
}

func parseIndexBlock(data []byte) *sparseindex.SparseIndex {
	r := data
	step, n := binary.Uvarint(r)
	if n <= 0 {
		return sparseindex.New(128)
	}
	r = r[n:]
	count, n := binary.Uvarint(r)
	if n <= 0 {
		return sparseindex.New(int(step))
	}
	r = r[n:]

	si := sparseindex.New(int(step))
	for i := uint64(0); i < count; i++ {
		kLen, n := binary.Uvarint(r)
		if n <= 0 || int(kLen) > len(r)-n {
			break
		}
		r = r[n:]
		key := string(r[:kLen])
		r = r[kLen:]

		off, n := binary.Uvarint(r)
		if n <= 0 {
			break
		}
		r = r[n:]
		si.Add(key, int64(off))
	}
	return si
}

func parseBloomBlock(data []byte, length int64) *bloom.Filter {
	if length <= 0 {
		return nil
	}
	buf := data[:length]

	if len(buf) == 1 && buf[0] == 0 {
		return nil
	}
	return bloom.Deserialize(buf)
}

func (s *SSTable) Get(key string) (memtable.Entry, bool) {
	if s.bloomFilter != nil && !s.bloomFilter.MayContain(key) {
		return memtable.Entry{}, false
	}

	offset := s.sparseIndex.Lookup(key)
	buf := s.data[offset:]

	pos := 0
	for pos < len(buf) {
		kLen, n := binary.Uvarint(buf[pos:])
		if n <= 0 || int(kLen) > len(buf[pos+n:]) {
			break
		}
		pos += n
		thisKey := string(buf[pos : pos+int(kLen)])
		pos += int(kLen)

		if thisKey > key {
			break
		}

		vLen, n := binary.Uvarint(buf[pos:])
		if n <= 0 || int(vLen) > len(buf[pos+n:]) {
			break
		}
		pos += n
		value := string(buf[pos : pos+int(vLen)])
		pos += int(vLen)

		ts, n := binary.Varint(buf[pos:])
		if n <= 0 {
			break
		}
		pos += n

		if pos >= len(buf) {
			break
		}
		tombstone := buf[pos] == 1
		pos++

		pos += 4
		if pos > len(buf) {
			break
		}

		if thisKey == key {
			return memtable.Entry{
				Key:       thisKey,
				Value:     value,
				Timestamp: ts,
				Tombstone: tombstone,
			}, true
		}
	}

	return memtable.Entry{}, false
}

func (s *SSTable) MayContain(key string) bool {
	return s.bloomFilter == nil || s.bloomFilter.MayContain(key)
}

func (s *SSTable) AllEntries() ([]memtable.Entry, error) {
	if s.data == nil {
		return nil, fmt.Errorf("sstable %s: not mmap'd", s.path)
	}
	var result []memtable.Entry
	buf := s.data[:s.dataSize]

	pos := 0
	for pos < len(buf) {
		entry, n := parseEntry(buf[pos:])
		if n <= 0 {
			break
		}
		result = append(result, entry)
		pos += n
	}
	return result, nil
}

func parseEntry(buf []byte) (memtable.Entry, int) {
	start := 0

	kLen, n := binary.Uvarint(buf)
	if n <= 0 || int(kLen) > len(buf)-n {
		return memtable.Entry{}, 0
	}
	start += n
	key := string(buf[start : start+int(kLen)])
	start += int(kLen)

	vLen, n := binary.Uvarint(buf[start:])
	if n <= 0 || int(vLen) > len(buf)-start-n {
		return memtable.Entry{}, 0
	}
	start += n
	value := string(buf[start : start+int(vLen)])
	start += int(vLen)

	ts, n := binary.Varint(buf[start:])
	if n <= 0 {
		return memtable.Entry{}, 0
	}
	start += n

	if start >= len(buf) {
		return memtable.Entry{}, 0
	}
	tombstone := buf[start] == 1
	start++

	start += 4
	if start > len(buf) {
		return memtable.Entry{}, 0
	}

	return memtable.Entry{
		Key:       key,
		Value:     value,
		Timestamp: ts,
		Tombstone: tombstone,
	}, start
}

type EntryIter struct {
	sst *SSTable
	pos int
}

func (s *SSTable) Iter() *EntryIter {
	return &EntryIter{sst: s}
}

func (it *EntryIter) Next() (memtable.Entry, bool) {
	if it == nil || it.sst == nil || it.pos >= int(it.sst.dataSize) {
		return memtable.Entry{}, false
	}
	entry, n := parseEntry(it.sst.data[it.pos:])
	if n <= 0 {
		return memtable.Entry{}, false
	}
	it.pos += n
	return entry, true
}

func (s *SSTable) Close() error {
	if s.data != nil {
		syscall.Munmap(s.data)
		s.data = nil
	}
	return s.file.Close()
}

func (s *SSTable) Path() string {
	return s.path
}

func (s *SSTable) DataSize() int64 {
	return s.dataSize
}

func (s *SSTable) SparseIndex() *sparseindex.SparseIndex {
	return s.sparseIndex
}

func (s *SSTable) BloomFilter() *bloom.Filter {
	return s.bloomFilter
}

func (s *SSTable) MinKey() string {
	entries := s.sparseIndex.Entries()
	if len(entries) == 0 {
		return ""
	}
	return entries[0].Key
}

func (s *SSTable) MaxKey() string {
	entries := s.sparseIndex.Entries()
	if len(entries) == 0 {
		return ""
	}
	return entries[len(entries)-1].Key
}
