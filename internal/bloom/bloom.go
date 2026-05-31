package bloom

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
)

type Filter struct {
	bits    []uint64
	numBits uint64
	numHash int
}

func New(numKeys int, bitsPerKey int) *Filter {
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}
	if numKeys <= 0 {
		numKeys = 1
	}

	numBits := uint64(numKeys * bitsPerKey)
	numBits = nextPowerOf2(numBits)
	if numBits < 64 {
		numBits = 64
	}

	numHash := int(math.Ceil(0.6931471805599453 * float64(numBits) / float64(numKeys)))
	if numHash < 1 {
		numHash = 1
	}

	return &Filter{
		bits:    make([]uint64, numBits/64),
		numBits: numBits,
		numHash: numHash,
	}
}

func NewWithBits(bits []uint64, numBits uint64, numHash int) *Filter {
	return &Filter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}
}

func (f *Filter) Add(key string) {
	h1, h2 := hashKey(key)
	delta := h2
	if delta == 0 {
		delta = 1
	}

	for i := 0; i < f.numHash; i++ {
		bit := (h1 + uint64(i)*delta) & (f.numBits - 1)
		f.bits[bit/64] |= 1 << (bit % 64)
	}
}

func (f *Filter) MayContain(key string) bool {
	h1, h2 := hashKey(key)
	delta := h2
	if delta == 0 {
		delta = 1
	}

	for i := 0; i < f.numHash; i++ {
		bit := (h1 + uint64(i)*delta) & (f.numBits - 1)
		if f.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (f *Filter) NumHash() int {
	return f.numHash
}

func (f *Filter) Bits() []uint64 {
	return f.bits
}

func (f *Filter) NumBits() uint64 {
	return f.numBits
}

func Serialize(f *Filter) []byte {
	buf := make([]byte, 0, 16+len(f.bits)*8)
	buf = binary.AppendUvarint(buf, f.numBits)
	buf = binary.AppendUvarint(buf, uint64(f.numHash))
	for _, v := range f.bits {
		buf = binary.LittleEndian.AppendUint64(buf, v)
	}
	return buf
}

func Deserialize(data []byte) *Filter {
	r := data
	numBits, n := binary.Uvarint(r)
	if n <= 0 {
		return nil
	}
	r = r[n:]
	numHash, n := binary.Uvarint(r)
	if n <= 0 {
		return nil
	}
	r = r[n:]

	numWords := numBits / 64
	bits := make([]uint64, numWords)
	for i := range bits {
		if len(r) < 8 {
			break
		}
		bits[i] = binary.LittleEndian.Uint64(r)
		r = r[8:]
	}

	return NewWithBits(bits, numBits, int(numHash))
}

func hashKey(key string) (uint64, uint64) {
	var h hash.Hash64 = fnv.New64a()
	h.Write([]byte(key))
	h1 := h.Sum64()

	h.Reset()
	h.Write([]byte("LSM"))
	h.Write([]byte(key))
	h2 := h.Sum64()

	return h1, h2
}

func nextPowerOf2(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}
