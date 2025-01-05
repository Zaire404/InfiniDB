package util

import "sync/atomic"

type Arena struct {
	// The size of the memory block in bytes.
	alloc_offset uint32
	// The memory block.
	blocks []byte
}

const (
	kBlockSize = 4096
)

func newArena(size uint32) *Arena {
	return &Arena{
		alloc_offset: 1,
		blocks:       make([]byte, size),
	}
}

// Allocate allocates a memory block of the given size.
// Return the beginning offset of the memory block.
func (a *Arena) allocate(size uint32) uint32 {
	offset := atomic.AddUint32(&a.alloc_offset, uint32(size))
	blocks_size := uint32(len(a.blocks))
	if offset > blocks_size {
		a.allocateFallback(size)
	}
	return offset - size
}

func (a *Arena) allocateFallback(size uint32) {
	blocks_size := uint32(len(a.blocks))
	grow_size := blocks_size
	if blocks_size > kBlockSize {
		// The memory block is too large.
		grow_size = size
	}
	newBlocks := make([]byte, blocks_size+grow_size)
	copy(newBlocks, a.blocks)
	a.blocks = newBlocks
}
