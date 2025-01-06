package lsm

import (
	"unsafe"

	"github.com/Zaire404/ZDB/util"
)

const (
	maxHeight  = 12
	uint32Size = uint32(unsafe.Sizeof(uint32(0)))
)

type node struct {
	//  ┌─────────────┬───────────┐
	//  │ valueOffset │ valueSize │
	//  └─────────────┴───────────┘
	value     uint64
	keySize   uint32
	keyOffset uint32
	height    uint32
	next      [maxHeight]uint32
}

type skiplist struct {
	height     uint32
	headOffset uint64
	arena      *util.Arena
}

func encodeValue(offset uint32, size uint32) uint64 {
	return uint64(offset)<<32 | uint64(size)
}

func decodeValue(value uint64) (offset uint32, size uint32) {
	return uint32(value >> 32), uint32(value)
}

func newNode(arena *util.Arena, key []byte, v util.ValueStruct, height uint32) *node {
	valueSize := len(v.Value)
	valueOffset := arena.Allocate(uint32(valueSize))
	keySize := len(key)
	keyOffset := arena.Allocate(uint32(keySize))
	nodeOffset := putNode(arena, height)

	node := getNode(arena, nodeOffset)
	node.value = encodeValue(valueOffset, uint32(valueSize))
	node.keySize = uint32(keySize)
	node.keyOffset = keyOffset
	node.height = height
	return node
}

func putNode(arena *util.Arena, height uint32) uint32 {
	size := (height + 5) * uint32Size
	offset := arena.Allocate(size)
	return offset
}

func getNode(arena *util.Arena, offset uint32) *node {
	return (*node)(unsafe.Pointer(&arena.Get(offset, 1)[0]))
}

func getNodeOffset(arena *util.Arena, node *node) uint32 {
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&arena.Get(0, 1)[0])))
}

func newSkiplist(arenaSize uint32) *skiplist {
	arena := util.NewArena(arenaSize)
	head := newNode(arena, nil, util.ValueStruct{}, maxHeight)
	return &skiplist{
		headOffset: uint64(getNodeOffset(arena, head)),
		arena:      arena,
		height:     1,
	}
}
