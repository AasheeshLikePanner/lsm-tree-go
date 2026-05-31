//go:build !linux

package wal

import "os"

func syncFile(f *os.File) error {
	return f.Sync()
}
