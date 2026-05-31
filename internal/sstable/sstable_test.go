package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/aasheesh/lsmtree/internal/memtable"
)

func TestSSTableWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.sst"

	n := 1000
	w, err := CreateWriter(path, n, 128)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := w.Append(memtable.Entry{
			Key:       fmtKey(i),
			Value:     fmtVal(i),
			Timestamp: int64(i * 100),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	sst, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	for i := 0; i < 100; i++ {
		idx := rand.Intn(n)
		e, ok := sst.Get(fmtKey(idx))
		if !ok {
			t.Fatalf("key %s not found", fmtKey(idx))
		}
		if e.Value != fmtVal(idx) {
			t.Fatalf("key %s: expected value %s, got %s", fmtKey(idx), fmtVal(idx), e.Value)
		}
		if e.Timestamp != int64(idx*100) {
			t.Fatalf("key %s: expected timestamp %d, got %d", fmtKey(idx), idx*100, e.Timestamp)
		}
	}
}

func TestSSTableEdgeKeys(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/edge.sst"

	w, err := CreateWriter(path, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []memtable.Entry{
		{Key: "aaa", Value: "1", Timestamp: 1},
		{Key: "bbb", Value: "2", Timestamp: 2},
		{Key: "ccc", Value: "3", Timestamp: 3, Tombstone: true},
	} {
		w.Append(e)
	}
	w.Close()

	sst, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	e, ok := sst.Get("aaa")
	if !ok || e.Value != "1" {
		t.Fatalf("expected aaa=1, got ok=%v val=%s", ok, e.Value)
	}

	e, ok = sst.Get("bbb")
	if !ok || e.Value != "2" {
		t.Fatalf("expected bbb=2, got ok=%v val=%s", ok, e.Value)
	}

	e, ok = sst.Get("ccc")
	if !ok || !e.Tombstone {
		t.Fatalf("expected ccc tombstone, got ok=%v tomb=%v", ok, e.Tombstone)
	}

	_, ok = sst.Get("zzz")
	if ok {
		t.Fatal("expected zzz to not be found")
	}
}

func TestSSTableExactSizes(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/size.sst"

	w, err := CreateWriter(path, 100, 5)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		w.Append(memtable.Entry{
			Key:       fmtKey(i),
			Value:     fmtVal(i),
			Timestamp: int64(i),
		})
	}
	w.Close()

	sst, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	for i := 0; i < 100; i++ {
		e, ok := sst.Get(fmtKey(i))
		if !ok {
			t.Fatalf("missing key %s", fmtKey(i))
		}
		if e.Timestamp != int64(i) {
			t.Fatalf("wrong timestamp for key %s: got %d want %d", fmtKey(i), e.Timestamp, i)
		}
	}
}

func TestSSTableFileClosed(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/close.sst"

	w, err := CreateWriter(path, 1, 128)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(memtable.Entry{Key: "k", Value: "v", Timestamp: 1})
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < footerSize {
		t.Fatalf("file too small: %d bytes", len(data))
	}
}

func fmtKey(i int) string { return fmt.Sprintf("key-%08d", i) }
func fmtVal(i int) string { return fmt.Sprintf("val-%08d", i) }
