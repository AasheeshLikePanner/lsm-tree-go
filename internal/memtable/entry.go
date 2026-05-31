package memtable

import "sort"

type Entry struct {
	Key       string
	Value     string
	Timestamp int64
	Tombstone bool
}

type MemTable struct {
	m     map[string]Entry
	keys  []string
	size  int
	dirty bool
}

func New() *MemTable {
	return &MemTable{
		m: make(map[string]Entry, 200000),
	}
}

func (m *MemTable) Put(key, value string, timestamp int64) {
	newSize := len(key) + len(value) + 16

	if old, exists := m.m[key]; exists {
		m.size -= len(key) + len(old.Value) + 16
	} else {
		m.dirty = true
	}

	m.m[key] = Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: false,
	}
	m.size += newSize
}

func (m *MemTable) Delete(key string, timestamp int64) {
	if old, exists := m.m[key]; exists {
		m.size -= len(key) + len(old.Value) + 16
	} else {
		m.dirty = true
	}

	m.m[key] = Entry{
		Key:       key,
		Timestamp: timestamp,
		Tombstone: true,
	}
	m.size += len(key) + 16
}

func (m *MemTable) Get(key string) (Entry, bool) {
	e, ok := m.m[key]
	return e, ok
}

func (m *MemTable) Entries() []Entry {
	if m.dirty || m.keys == nil {
		m.keys = make([]string, 0, len(m.m))
		for k := range m.m {
			m.keys = append(m.keys, k)
		}
		sort.Strings(m.keys)
		m.dirty = false
	}

	result := make([]Entry, len(m.keys))
	for i, k := range m.keys {
		result[i] = m.m[k]
	}
	return result
}

func (m *MemTable) Size() int {
	return m.size
}

func (m *MemTable) Count() int {
	return len(m.m)
}
