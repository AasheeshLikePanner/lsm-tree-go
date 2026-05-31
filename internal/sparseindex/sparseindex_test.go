package sparseindex

import (
	"fmt"
	"testing"
)

func TestSparseIndexLookup(t *testing.T) {
	si := New(128)
	for i := 0; i < 10000; i++ {
		if i%128 == 0 {
			si.Add(fmtKey(i), int64(i*64))
		}
	}

	for i := 0; i < 10000; i++ {
		offset := si.Lookup(fmtKey(i))
		if offset > int64(i*64) {
			t.Fatalf("key %s: offset %d > expected max %d", fmtKey(i), offset, i*64)
		}
	}
}

func TestSparseIndexEmpty(t *testing.T) {
	si := New(128)
	if offset := si.Lookup("anything"); offset != 0 {
		t.Fatalf("expected 0, got %d", offset)
	}
}

func TestSparseIndexExact(t *testing.T) {
	si := New(10)
	si.Add("key-00050", 500)
	si.Add("key-00100", 1000)

	if offset := si.Lookup("key-00050"); offset != 500 {
		t.Fatalf("expected 500, got %d", offset)
	}
	if offset := si.Lookup("key-00075"); offset != 500 {
		t.Fatalf("expected 500 (before key-00100), got %d", offset)
	}
	if offset := si.Lookup("key-00200"); offset != 1000 {
		t.Fatalf("expected 1000 (last entry), got %d", offset)
	}
	if offset := si.Lookup("key-00000"); offset != 0 {
		t.Fatalf("expected 0 (before first entry), got %d", offset)
	}
}

func fmtKey(i int) string { return fmt.Sprintf("key-%08d", i) }
