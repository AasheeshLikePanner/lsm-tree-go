package sparseindex

import "sort"

type Entry struct {
	Key    string
	Offset int64
}

type SparseIndex struct {
	entries []Entry
	step    int
}

func New(step int) *SparseIndex {
	if step <= 0 {
		step = 128
	}
	return &SparseIndex{step: step}
}

func (s *SparseIndex) ShouldIndex(count int) bool {
	return count >= 0 && count%s.step == 0
}

func (s *SparseIndex) Add(key string, offset int64) {
	s.entries = append(s.entries, Entry{Key: key, Offset: offset})
}

func (s *SparseIndex) Lookup(key string) int64 {
	if len(s.entries) == 0 {
		return 0
	}

	n := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].Key >= key
	})

	if n == len(s.entries) {
		return s.entries[len(s.entries)-1].Offset
	}
	if s.entries[n].Key == key {
		return s.entries[n].Offset
	}
	if n == 0 {
		return 0
	}
	return s.entries[n-1].Offset
}

func (s *SparseIndex) Entries() []Entry {
	return s.entries
}

func (s *SparseIndex) Step() int {
	return s.step
}

func (s *SparseIndex) Count() int {
	return len(s.entries)
}
