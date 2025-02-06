package util

import (
	"math"
)

const (
	log2          float64 = 0.6931471805599453
	minNumBits    uint32  = 64
	minNumHash    uint32  = 1
	maxNumHash    uint32  = 30
	minBitsPerKey         = 1
)

// ┌────────────┬───────────┐
// │   bitmap   │  numHash  │
// └────────────┴───────────┘
type BloomFilter []byte

func (bf *BloomFilter) MayContain(key uint32) bool {
	numBytes := len(*bf) - 1
	numHash := uint32((*bf)[numBytes])
	numBits := uint32(numBytes) * 8
	delta := key>>17 | key<<15
	for i := uint32(0); i < numHash; i++ {
		pos := key % numBits
		if (*bf)[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		key += delta
	}
	return true
}

// NewBloomFilter creates a new BloomFilter with the given keys and false positive probability.
// keys is hash values of the keys to be inserted (murMurHash3_x86_32).
func NewBloomFilter(keys []uint32, fp float64) *BloomFilter {
	numKeys := uint32(len(keys))
	bitsPerKey := bitsPerKey(numKeys, fp)
	// bitsPerKey := uint32(10)
	numBytes := calNumBytes(numKeys, bitsPerKey)
	numHash := calNumHash(bitsPerKey)
	numBits := numBytes * 8
	bf := BloomFilter(make([]byte, numBytes+1))
	bf[numBytes] = byte(numHash)
	for _, key := range keys {
		bf.insert(key, numHash, numBits)
	}
	return &bf
}

// bitsPerKey returns the number of bits per key for a given number of keys and false positive probability.
func bitsPerKey(numKeys uint32, fp float64) uint32 {
	size := -1 * float64(numKeys) * math.Log(fp) / math.Pow(log2, 2)
	locs := math.Ceil(size / float64(numKeys))
	if locs < minBitsPerKey {
		locs = minBitsPerKey
	}
	return uint32(locs)
}

// calNumHash returns the number of hash functions for a given number of keys and bits per key.
func calNumHash(bitsPerKey uint32) uint32 {
	res := uint32(float64(bitsPerKey) * log2)
	if res < minNumHash {
		res = minNumHash
	}
	if res > maxNumHash {
		res = maxNumHash
	}
	return res
}

// calNumBytes returns the number of bytes for a given number of keys and bits per key.
func calNumBytes(numKeys uint32, bitsPerKey uint32) uint32 {
	numBits := numKeys * bitsPerKey
	if numBits < minNumBits {
		numBits = minNumBits
	}
	numBytes := (numBits + 7) / 8
	return numBytes
}

// insert inserts a key into the BloomFilter.
func (bf *BloomFilter) insert(key uint32, numHash uint32, numBits uint32) {
	delta := key>>17 | key<<15
	for i := uint32(0); i < numHash; i++ {
		pos := key % numBits
		(*bf)[pos/8] |= 1 << (pos % 8)
		key += delta
	}
}

func (bf *BloomFilter) Len() int {
	return len(*bf)
}

func (bf *BloomFilter) Bytes() []byte {
	return []byte(*bf)
}
