package cache

import (
	"math"

	"github.com/Zaire404/InfiniDB/util"
)

const (
	log2          float64 = 0.6931471805599453
	minNumBits    uint32  = 64
	minNumHash    uint32  = 1
	maxNumHash    uint32  = 30
	minBitsPerKey         = 1
)

type BloomFilter struct {
	bitmap  []byte
	numHash uint32
}

func (bf *BloomFilter) MayContain(key uint32) bool {
	numBytes := len(bf.bitmap)
	numBits := uint32(numBytes) * 8
	delta := key>>17 | key<<15
	for i := uint32(0); i < bf.numHash; i++ {
		pos := key % numBits
		if bf.bitmap[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		key += delta
	}
	return true
}

func (bf *BloomFilter) MayContainKey(key []byte) bool {
	hash := util.Hash(key)
	return bf.MayContain(hash)
}

// NewBloomFilter creates a new BloomFilter with the given size and false positive probability.
func NewBloomFilter(size int, fp float64) *BloomFilter {
	// numKeys := uint32(len(keys))
	bitsPerKey := bitsPerKey(uint32(size), fp)
	// bitsPerKey := uint32(10)
	numBytes := calNumBytes(uint32(size), bitsPerKey)
	numHash := calNumHash(bitsPerKey)
	return &BloomFilter{
		bitmap:  make([]byte, numBytes),
		numHash: numHash,
	}
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
func (bf *BloomFilter) insert(key uint32) {
	numBits := uint32(len(bf.bitmap)) * 8
	delta := key>>17 | key<<15
	for i := uint32(0); i < bf.numHash; i++ {
		pos := key % numBits
		bf.bitmap[pos/8] |= 1 << (pos % 8)
		key += delta
	}
}

func (bf *BloomFilter) Len() int {
	return len(bf.bitmap)
}

func (bf *BloomFilter) Bytes() []byte {
	return bf.bitmap
}

func (bf *BloomFilter) Allow(key uint32) bool {
	if bf.MayContain(key) {
		return true
	}
	bf.insert(key)
	return false
}

func (bf *BloomFilter) AllowKey(key []byte) bool {
	hash := util.Hash(key)
	return bf.Allow(hash)
}

func (bf *BloomFilter) Reset() {
	for i := range bf.bitmap {
		bf.bitmap[i] = 0
	}
}
