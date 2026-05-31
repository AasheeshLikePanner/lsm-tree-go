//go:build linux

package wal

import (
	"fmt"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	IORING_OP_NOP    = 0
	IORING_OP_READV  = 1
	IORING_OP_WRITEV = 2
	IORING_OP_FSYNC  = 3
	IORING_OP_READ   = 4
	IORING_OP_WRITE  = 5
)

const (
	IORING_ENTER_GETEVENTS  = 1 << 0
	IORING_SETUP_SQPOLL     = 1 << 1
	IORING_SETUP_NO_SQARRAY = 1 << 12
)

const (
	SYS_IO_URING_SETUP    = 425
	SYS_IO_URING_ENTER    = 426
	SYS_IO_URING_REGISTER = 427
)

const (
	IORING_OFF_SQ_RING = 0
	IORING_OFF_CQ_RING = 0x8000000
	IORING_OFF_SQES    = 0x10000000
)

const iouringDepth = 256

type io_uring_sqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	rwFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	fileIndex   uint32
	addr3       uint64
	pad2        uint64
}

type io_uring_cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

type io_sqring_offsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

type io_cqring_offsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

type io_uring_params struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        io_sqring_offsets
	cqOff        io_cqring_offsets
}

func init() {
	sqe := io_uring_sqe{}
	if unsafe.Sizeof(sqe) != 64 {
		panic(fmt.Sprintf("io_uring_sqe size = %d, want 64", unsafe.Sizeof(sqe)))
	}
	if unsafe.Offsetof(sqe.opcode) != 0 {
		panic("io_uring_sqe.opcode offset != 0")
	}
	if unsafe.Offsetof(sqe.flags) != 1 {
		panic("io_uring_sqe.flags offset != 1")
	}
	if unsafe.Offsetof(sqe.fd) != 4 {
		panic("io_uring_sqe.fd offset != 4")
	}
	if unsafe.Offsetof(sqe.off) != 8 {
		panic("io_uring_sqe.off offset != 8")
	}
	if unsafe.Offsetof(sqe.addr) != 16 {
		panic("io_uring_sqe.addr offset != 16")
	}
	if unsafe.Offsetof(sqe.len) != 24 {
		panic("io_uring_sqe.len offset != 24")
	}
	if unsafe.Offsetof(sqe.userData) != 32 {
		panic("io_uring_sqe.userData offset != 32")
	}
	cqe := io_uring_cqe{}
	if unsafe.Sizeof(cqe) != 16 {
		panic(fmt.Sprintf("io_uring_cqe size = %d, want 16", unsafe.Sizeof(cqe)))
	}
	if unsafe.Offsetof(cqe.userData) != 0 {
		panic("io_uring_cqe.userData offset != 0")
	}
	if unsafe.Offsetof(cqe.res) != 8 {
		panic("io_uring_cqe.res offset != 8")
	}
}

type iouring struct {
	ringFD      int
	sqRing      []byte
	cqRing      []byte
	sqes        []byte
	sqHead      *uint32
	sqTail      *uint32
	sqMask      *uint32
	sqEntries   uint32
	cqHead      *uint32
	cqTail      *uint32
	cqMask      *uint32
	cqEntries   uint32
	cqesOff     uint32
	fd          int
	offset      int64
	sqLocalTail uint32

	writeBufs [][]byte
	wbHead    int
	wbTail    int
}

func newIORing(fd int) (*iouring, error) {
	params := &io_uring_params{flags: IORING_SETUP_NO_SQARRAY}
	r1, _, e := syscall.Syscall(SYS_IO_URING_SETUP, iouringDepth, uintptr(unsafe.Pointer(params)), 0)
	if e != 0 {
		params = &io_uring_params{}
		r1, _, e = syscall.Syscall(SYS_IO_URING_SETUP, iouringDepth, uintptr(unsafe.Pointer(params)), 0)
		if e != 0 {
			return nil, fmt.Errorf("io_uring_setup: %w", e)
		}
	}
	ringFD := int(r1)

	r := &iouring{
		ringFD:    ringFD,
		fd:        fd,
		sqEntries: params.sqEntries,
		cqEntries: params.cqEntries,
		writeBufs: make([][]byte, iouringDepth),
	}

	sqRingSize := int(params.sqOff.array + params.sqEntries*4)
	cqRingSize := int(params.cqOff.cqes + params.cqEntries*16)
	sqeSize := int(params.sqEntries * uint32(unsafe.Sizeof(io_uring_sqe{})))

	var err error
	r.sqRing, err = syscall.Mmap(ringFD, IORING_OFF_SQ_RING, sqRingSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(ringFD)
		return nil, fmt.Errorf("mmap sq_ring: %w", err)
	}
	r.cqRing, err = syscall.Mmap(ringFD, IORING_OFF_CQ_RING, cqRingSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Munmap(r.sqRing)
		syscall.Close(ringFD)
		return nil, fmt.Errorf("mmap cq_ring: %w", err)
	}
	r.sqes, err = syscall.Mmap(ringFD, IORING_OFF_SQES, sqeSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Munmap(r.cqRing)
		syscall.Munmap(r.sqRing)
		syscall.Close(ringFD)
		return nil, fmt.Errorf("mmap sqes: %w", err)
	}

	r.sqHead = (*uint32)(unsafe.Pointer(&r.sqRing[params.sqOff.head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&r.sqRing[params.sqOff.tail]))
	r.sqMask = (*uint32)(unsafe.Pointer(&r.sqRing[params.sqOff.ringMask]))

	r.cqHead = (*uint32)(unsafe.Pointer(&r.cqRing[params.cqOff.head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&r.cqRing[params.cqOff.tail]))
	r.cqMask = (*uint32)(unsafe.Pointer(&r.cqRing[params.cqOff.ringMask]))
	r.cqesOff = params.cqOff.cqes

	return r, nil
}

func (r *iouring) submitWrite(data []byte) error {

	submitted := r.sqLocalTail - *r.sqHead
	if submitted+2 > r.sqEntries {
		if err := r.flush(); err != nil {
			return err
		}
	}

	buf := make([]byte, len(data))
	copy(buf, data)

	idx := r.wbTail % len(r.writeBufs)
	r.writeBufs[idx] = buf
	r.wbTail++

	userData := uint64(idx + 1)
	mask := *r.sqMask

	slot := r.sqLocalTail & mask
	sqe := (*io_uring_sqe)(unsafe.Pointer(&r.sqes[slot*64]))
	sqe.opcode = IORING_OP_WRITE
	sqe.flags = 0
	sqe.fd = int32(r.fd)
	sqe.off = uint64(r.offset)
	sqe.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
	sqe.userData = userData

	slot2 := (r.sqLocalTail + 1) & mask
	sqe2 := (*io_uring_sqe)(unsafe.Pointer(&r.sqes[slot2*64]))
	sqe2.opcode = IORING_OP_FSYNC
	sqe2.flags = 0
	sqe2.fd = int32(r.fd)
	sqe2.off = 0
	sqe2.addr = 0
	sqe2.len = 0
	sqe2.userData = userData | 0x80000000

	r.sqLocalTail += 2
	atomic.StoreUint32(r.sqTail, r.sqLocalTail)
	r.offset += int64(len(data))
	return nil
}

func (r *iouring) flush() error {
	head := *r.sqHead
	tail := *r.sqTail
	toSubmit := tail - head
	if toSubmit == 0 {
		return nil
	}

	_, _, err := syscall.Syscall6(SYS_IO_URING_ENTER,
		uintptr(r.ringFD),
		uintptr(toSubmit),
		uintptr(toSubmit),
		uintptr(IORING_ENTER_GETEVENTS),
		0, 0,
	)
	if err != 0 {
		return err
	}

	return r.reap(int(toSubmit))
}

func (r *iouring) reap(count int) error {
	for count > 0 {
		head := *r.cqHead
		tail := atomic.LoadUint32(r.cqTail)

		if head == tail {
			_, _, e := syscall.Syscall6(SYS_IO_URING_ENTER,
				uintptr(r.ringFD),
				0,
				uintptr(count),
				uintptr(IORING_ENTER_GETEVENTS),
				0, 0,
			)
			if e != 0 {
				if e == syscall.EINTR {
					continue
				}
				return e
			}
			tail = atomic.LoadUint32(r.cqTail)
		}

		n := tail - head
		if uint32(count) < n {
			n = uint32(count)
		}
		for i := uint32(0); i < n; i++ {
			cqe := (*io_uring_cqe)(unsafe.Pointer(&r.cqRing[r.cqesOff+uint32(head&*r.cqMask)*16]))
			if cqe.res < 0 {
				atomic.StoreUint32(r.cqHead, head+i+1)
				return syscall.Errno(-cqe.res)
			}
			idx := int(cqe.userData&0x7fffffff) - 1
			if idx >= 0 && idx < len(r.writeBufs) {
				r.writeBufs[idx] = nil
				r.wbHead++
			}
		}
		head += n
		atomic.StoreUint32(r.cqHead, head)
		count -= int(n)
	}
	return nil
}

func (r *iouring) close() {
	r.flush()
	if r.sqes != nil {
		syscall.Munmap(r.sqes)
	}
	if r.cqRing != nil {
		syscall.Munmap(r.cqRing)
	}
	if r.sqRing != nil {
		syscall.Munmap(r.sqRing)
	}
	if r.ringFD > 0 {
		syscall.Close(r.ringFD)
	}
}
