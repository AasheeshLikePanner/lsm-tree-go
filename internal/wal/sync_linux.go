//go:build linux

package wal

import (
	"os"
	"syscall"
)

func syncFile(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
