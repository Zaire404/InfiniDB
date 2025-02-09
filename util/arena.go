package util

import (
	"sync/atomic"
)

type Arena struct {
	// The size of the memory block in bytes.
	allocOffset uint32
	// The memory block.
	blocks []byte
}

const (
	kBlockSize = 4096
)

func NewArena(size uint32) *Arena {
	return &Arena{
		allocOffset: 1,
		blocks:      make([]byte, size),
	}
}

// Allocate allocates a memory block of the given size.
// Return the beginning offset of the memory block.
func (a *Arena) Allocate(size uint32) uint32 {
	offset := atomic.AddUint32(&a.allocOffset, uint32(size))
	blocks_size := uint32(len(a.blocks))
	if offset > blocks_size {
		a.allocateFallback(size)
	}
	return offset - size
}

func (a *Arena) allocateFallback(size uint32) {
	blocks_size := uint32(len(a.blocks))
	grow_size := blocks_size
	// TODO: The memory block size should be dynamically adjusted.
	if blocks_size > kBlockSize {
		// The memory block is too large.
		grow_size = size
	}
	newBlocks := make([]byte, blocks_size+grow_size)
	copy(newBlocks, a.blocks)
	a.blocks = newBlocks
}

func (a *Arena) Get(offset uint32, size uint32) []byte {
	return a.blocks[offset : offset+size]
}

func (a *Arena) Size() uint32 {
	return atomic.LoadUint32(&a.allocOffset)
}

func (a *Arena) Checksum() uint32 {
	checksum := Checksum(a.blocks[:a.allocOffset])
	return checksum
}

func RecoverArena(data []byte, len uint32) *Arena {
	a := NewArena(len)
	copy(a.blocks, data)
	a.allocOffset = len
	return a
}
