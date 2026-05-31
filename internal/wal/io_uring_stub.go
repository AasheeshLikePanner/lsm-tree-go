//go:build !linux

package wal

import "errors"

type iouring struct{}

func newIORing(fd int) (*iouring, error) {
	return nil, errors.New("io_uring is only supported on Linux")
}

func (r *iouring) submitWrite(data []byte) error {
	return errors.New("io_uring not available")
}

func (r *iouring) flush() error {
	return errors.New("io_uring not available")
}

func (r *iouring) close() {}
