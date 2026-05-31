//go:build !linux

package sstable

import "syscall"

func syncFd(fd int) error {
	return syscall.Fsync(fd)
}
