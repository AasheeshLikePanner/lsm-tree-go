package memtable

import (
	"math/rand"
	"testing"
)

func TestMemTableSortedRead(t *testing.T) {
	m := New()
	const N = 10000

	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = randomString(8)
	}
	rand.Shuffle(N, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for i, k := range keys {
		m.Put(k, randomString(16), int64(i))
	}

	if m.Count() != N {
		t.Fatalf("expected %d entries, got %d", N, m.Count())
	}

	entries := m.Entries()
	for i := 1; i < len(entries); i++ {
		if entries[i].Key < entries[i-1].Key {
			t.Fatalf("entries not sorted at index %d: %s < %s", i, entries[i].Key, entries[i-1].Key)
		}
	}

	for i, k := range keys {
		e, ok := m.Get(k)
		if !ok {
			t.Fatalf("key %s not found (insertion order %d)", k, i)
		}
		if e.Timestamp != int64(i) {
			t.Fatalf("key %s: expected timestamp %d, got %d", k, i, e.Timestamp)
		}
	}
}

func TestMemTableUpdateWins(t *testing.T) {
	m := New()
	m.Put("x", "old", 100)
	m.Put("x", "new", 200)

	e, ok := m.Get("x")
	if !ok {
		t.Fatal("key x not found")
	}
	if e.Value != "new" {
		t.Fatalf("expected 'new', got '%s'", e.Value)
	}
	if e.Timestamp != 200 {
		t.Fatalf("expected timestamp 200, got %d", e.Timestamp)
	}
}

func TestMemTableSizeTracking(t *testing.T) {
	m := New()
	m.Put("abc", "123", 1)
	m.Put("def", "456", 2)

	expected := (3 + 3 + 16) + (3 + 3 + 16)
	if m.Size() != expected {
		t.Fatalf("expected size %d, got %d", expected, m.Size())
	}

	m.Put("abc", "longer-value", 3)
	expected = expected - (3 + 3 + 16) + (3 + 12 + 16)
	if m.Size() != expected {
		t.Fatalf("expected size %d after update, got %d", expected, m.Size())
	}
}

func TestMemTableDelete(t *testing.T) {
	m := New()
	m.Put("k", "v", 100)
	m.Delete("k", 200)

	e, ok := m.Get("k")
	if !ok {
		t.Fatal("key k should exist as tombstone")
	}
	if !e.Tombstone {
		t.Fatal("expected tombstone for deleted key")
	}
	if e.Timestamp != 200 {
		t.Fatalf("expected timestamp 200, got %d", e.Timestamp)
	}
}

func TestMemTableDeleteThenPut(t *testing.T) {
	m := New()
	m.Put("k", "v", 100)
	m.Delete("k", 200)
	m.Put("k", "restored", 300)

	e, ok := m.Get("k")
	if !ok {
		t.Fatal("key k should exist after delete+put")
	}
	if e.Value != "restored" {
		t.Fatalf("expected 'restored', got '%s'", e.Value)
	}
	if e.Tombstone {
		t.Fatal("key should not be tombstone after restore")
	}
}

func TestMemTableEmpty(t *testing.T) {
	m := New()
	if m.Count() != 0 {
		t.Fatalf("expected empty, got %d entries", m.Count())
	}
	if _, ok := m.Get("anything"); ok {
		t.Fatal("expected not found")
	}
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
