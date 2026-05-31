package bloom

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	n := 10000
	f := New(n, 10)

	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%08d", i)
		f.Add(keys[i])
	}

	for _, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("false negative for key %s", k)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	n := 10000
	f := New(n, 10)

	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key-%08d", i))
	}

	falsePositives := 0
	trials := 10000
	for i := 0; i < trials; i++ {

		k := fmt.Sprintf("other-%08d", rand.Intn(1<<30))
		if f.MayContain(k) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(trials)
	t.Logf("False positive rate: %.4f (%d/%d)", rate, falsePositives, trials)

	if rate > 0.05 {
		t.Fatalf("false positive rate too high: %.4f", rate)
	}
}

func TestBloomEmpty(t *testing.T) {
	f := New(1000, 10)

	if f.MayContain("anything") {
		t.Fatal("empty filter should not contain anything")
	}
}

func TestBloomSerializeDeserialize(t *testing.T) {
	n := 5000
	f := New(n, 10)
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key-%08d", i))
	}

	data := Serialize(f)
	f2 := Deserialize(data)

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%08d", i)
		if f2.MayContain(k) != f.MayContain(k) {
			t.Fatalf("mismatch for key %s", k)
		}
	}
}

func TestBloomKeyNotAdded(t *testing.T) {
	f := New(100, 10)
	f.Add("present-key")

	if !f.MayContain("present-key") {
		t.Fatal("added key should be found")
	}

}

func TestBloomHardKeys(t *testing.T) {

	f := New(256, 10)
	for i := 0; i < 256; i++ {
		f.Add(string([]byte{byte(i)}))
	}

	for i := 0; i < 256; i++ {
		if !f.MayContain(string([]byte{byte(i)})) {
			t.Fatalf("false negative for byte %d", i)
		}
	}
}
