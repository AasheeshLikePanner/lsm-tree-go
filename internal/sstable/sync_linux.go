//go:build linux

package sstable

import "syscall"

func syncFd(fd int) error {
	return syscall.Fdatasync(fd)
}
