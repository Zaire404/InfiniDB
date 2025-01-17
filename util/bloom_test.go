package util

import (
	"fmt"
	"testing"
)

func TestSmallBloomFilter(t *testing.T) {
	var hash []uint32
	for _, word := range []string{"hello", "world"} {
		hash = append(hash, Hash([]byte(word)))
	}
	bf := NewBloomFilter(hash, 0.01)
	for _, word := range []string{"hello", "world"} {
		if !bf.MayContain(Hash([]byte(word))) {
			t.Errorf("BloomFilter should contain %s", word)
		}
	}
}

func TestBloomFilter(t *testing.T) {
	nextLength := func(x int) int {
		if x < 10 {
			return x + 1
		}
		if x < 100 {
			return x + 10
		}
		if x < 1000 {
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte {
		b := make([]byte, 4)
		b[0] = uint8(uint32(i) >> 0)
		b[1] = uint8(uint32(i) >> 8)
		b[2] = uint8(uint32(i) >> 16)
		b[3] = uint8(uint32(i) >> 24)
		return b
	}

	nMediocreFilters, nGoodFilters := 0, 0
loop:
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := 0; i < length; i++ {
			keys = append(keys, le32(i))
		}
		var hashes []uint32
		for _, key := range keys {
			hashes = append(hashes, Hash(key))
		}
		f := NewBloomFilter(hashes, 0.0125)

		if f.Len() > (length*10/8)+40 {
			t.Errorf("length=%d: len(f)=%d is too large", length, f.Len())
			continue
		}

		// All added keys must match.
		for _, key := range keys {
			if !f.MayContain(Hash(key)) {
				t.Errorf("length=%d: did not contain key %q", length, key)
				continue loop
			}
		}

		// Check false positive rate.
		nFalsePositive := 0
		for i := 0; i < 10000; i++ {
			if f.MayContain(Hash(le32(1e9 + i))) {
				nFalsePositive++
			}
		}
		if nFalsePositive > 0.02*10000 {
			t.Errorf("length=%d: %d false positives in 10000", length, nFalsePositive)
			continue
		}
		if nFalsePositive > 0.0125*10000 {
			nMediocreFilters++
		} else {
			nGoodFilters++
		}
	}

	if nMediocreFilters > nGoodFilters/5 {
		t.Errorf("%d mediocre filters but only %d good filters", nMediocreFilters, nGoodFilters)
	}
}

func BenchmarkBloomFilter(b *testing.B) {
	falsePositiveRates := []float64{0.001, 0.01, 0.1}

	for _, fpr := range falsePositiveRates {
		b.Run(fmt.Sprintf("FPR_%.3f", fpr), func(b *testing.B) {
			const n = 100000
			const maxLenKey = 40
			keys := make([]uint32, 0, n)
			for i := 0; i < n; i++ {
				keys = append(keys, Hash([]byte(GetRandomString(GetRandomInt(maxLenKey)))))
			}
			bf := NewBloomFilter(keys, fpr)
			for _, key := range keys {
				if bf.MayContain(key) != true {
					b.Errorf("BloomFilter should contain %d", key)
				}
			}
		})
	}
}
