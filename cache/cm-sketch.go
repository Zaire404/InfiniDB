package cache

import "math/rand/v2"

type CountMinSketch struct {
	rows    []*cmRow
	numHash uint32
}

type cmRow struct {
	bitmap []byte
	seed   uint64
	mask   uint64
}

func newCmRow(numCounter int) *cmRow {
	numCounter = next2Power(numCounter)
	return &cmRow{
		bitmap: make([]byte, numCounter/2),
		seed:   rand.Uint64(),
		mask:   uint64(numCounter) - 1,
	}
}

func (cr *cmRow) add(key uint64) {
	counterIndex := (key ^ cr.seed) & cr.mask
	cr.incrCounter(int(counterIndex))
}

func (cr *cmRow) get(key uint64) uint8 {
	counterIndex := (key ^ cr.seed) & cr.mask
	return cr.getCounter(counterIndex)
}

func (cr *cmRow) getCounter(counterIndex uint64) uint8 {
	byteIndex := counterIndex / 2
	bitIndex := (counterIndex & 1) * 4
	return (cr.bitmap[byteIndex] >> bitIndex) & 0xf
}

// incrCounter increments the counter at the given index.
// The counter is a 4-bit unsigned integer.
// for example, counterIndex = 1, bitmap[0] == 1111 0000
// after incrCounter(1), bitmap[0] == 1111 0001
func (cr *cmRow) incrCounter(counterIndex int) {
	byteIndex := counterIndex / 2
	bitIndex := (counterIndex & 1) * 4
	counter := (cr.bitmap[byteIndex] >> bitIndex) & 0xf

	if counter < 15 {
		cr.bitmap[byteIndex] += 1 << bitIndex
	}
}

// refresh the row, every counter will be divided by 2
// for example, bitmap[0] == 1101 1001
// after reset(), bitmap[0] == 0110 0100
func (cr *cmRow) reset() {
	for i := range cr.bitmap {
		// 0x77 = 0111 0111
		cr.bitmap[i] = cr.bitmap[i] >> 1 & 0x77
	}
}

func NewCountMinSketch(numHash uint32, numCounter int) *CountMinSketch {
	if numCounter <= 0 {
		panic("numCounter must be positive")
	}
	rows := make([]*cmRow, numHash)
	for i := range rows {
		rows[i] = newCmRow(numCounter)
	}
	return &CountMinSketch{
		rows:    rows,
		numHash: numHash,
	}
}

func (cs *CountMinSketch) Add(key uint64) {
	for _, row := range cs.rows {
		row.add(key)
	}
}

// GetEstimate returns the estimated count of the given key.
func (cs *CountMinSketch) GetEstimate(key uint64) uint8 {
	min := uint8(255)
	for i := range cs.rows {
		val := cs.rows[i].get(key)
		if val < min {
			min = val
		}
	}
	return min
}

func (cs *CountMinSketch) Reset() {
	for _, row := range cs.rows {
		row.reset()
	}
}

// next2Power returns the next power of 2 of the given number.
func next2Power(num int) int {
	if num <= 0 {
		return 1
	}
	num--
	num |= num >> 1
	num |= num >> 2
	num |= num >> 4
	num |= num >> 8
	num |= num >> 16
	num++
	return num
}
