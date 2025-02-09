package lsm

import (
	"bytes"
	"math"
	"sync/atomic"
	"unsafe"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/util"
)

const (
	maxHeight      = 20
	uint32Size     = uint32(unsafe.Sizeof(uint32(0)))
	heightIncrease = math.MaxUint32 / 3
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

type SkipList struct {
	height     uint32
	headOffset uint32
	arena      *util.Arena
}

func (n *node) key(arena *util.Arena) []byte {
	return arena.Get(n.keyOffset, n.keySize)
}

func (n *node) vs(arena *util.Arena) util.ValueStruct {
	offset, size := decodeValue(n.value)
	var vs util.ValueStruct
	vs.DecodeValue(arena.Get(offset, size))
	return vs
}

func encodeValue(offset uint32, size uint32) uint64 {
	return uint64(offset)<<32 | uint64(size)
}

func decodeValue(value uint64) (offset uint32, size uint32) {
	return uint32(value >> 32), uint32(value)
}

func newNode(arena *util.Arena, key []byte, v util.ValueStruct, height uint32) *node {
	valueOffset := putValue(arena, v)
	keyOffset := putKey(arena, key)
	nodeOffset := putNode(arena, height)

	node := getNode(arena, nodeOffset)
	node.value = encodeValue(valueOffset, v.EncodedSize())
	node.keySize = uint32(len(key))
	node.keyOffset = keyOffset
	node.height = height
	return node
}

func putNode(arena *util.Arena, height uint32) uint32 {
	size := (height + 5) * uint32Size
	offset := arena.Allocate(size)
	return offset
}

func putKey(arena *util.Arena, key []byte) uint32 {
	size := len(key)
	offset := arena.Allocate(uint32(size))
	copy(arena.Get(offset, uint32(size)), key)
	return offset
}

func putValue(arena *util.Arena, v util.ValueStruct) uint32 {
	size := v.EncodedSize()
	offset := arena.Allocate(size)
	v.EncodeValue(arena.Get(offset, size))
	return offset
}

func (n *node) getNextOffset(level int) uint32 {
	return atomic.LoadUint32(&n.next[level])
}

func (n *node) casNextOffset(level int, old uint32, new uint32) bool {
	return atomic.CompareAndSwapUint32(&n.next[level], old, new)
}

func getNode(arena *util.Arena, offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&arena.Get(offset, 1)[0]))
}

func getNodeOffset(arena *util.Arena, node *node) uint32 {
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&arena.Get(0, 1)[0])))
}

func NewSkipList(arenaSize uint32) *SkipList {
	arena := util.NewArena(arenaSize)
	head := newNode(arena, nil, util.ValueStruct{}, maxHeight)
	return &SkipList{
		headOffset: getNodeOffset(arena, head),
		arena:      arena,
		height:     1,
	}
}

func (sl *SkipList) getHeight() uint32 {
	return atomic.LoadUint32(&sl.height)
}

// getNext returns the next node of the given node at the given level.
func (sl *SkipList) getNext(nd *node, level int) *node {
	return getNode(sl.arena, nd.getNextOffset(level))
}

// findSplice finds the node before and after the target key.
func (sl *SkipList) findSpliceForLevel(level int, key []byte, prev uint32) (uint32, uint32) {
	for {
		prevNode := getNode(sl.arena, prev)
		next := prevNode.getNextOffset(level)
		nextNode := getNode(sl.arena, next)
		if nextNode == nil {
			return prev, next
		}
		nextKey := nextNode.key(sl.arena)
		if bytes.Compare(nextKey, key) > 0 {
			return prev, next
		} else if bytes.Equal(nextKey, key) {
			return next, next
		}
		prev = next
	}
}

func (sl *SkipList) Add(e *util.Entry) {
	// Find the insertion point.
	prev := make([]uint32, maxHeight+1)
	next := make([]uint32, maxHeight+1)
	// before := sl.headOffset
	prev[sl.getHeight()] = sl.headOffset
	for i := int(sl.height) - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, prev[i+1])
		if prev[i] == next[i] {
			// The key already exists.Only update the value.
			node := getNode(sl.arena, prev[i])
			valueOffset := putValue(sl.arena, e.ValueStruct)
			node.value = encodeValue(valueOffset, e.ValueStruct.EncodedSize())
			return
		}
	}

	// Insert the new node.
	height := randomHeight()
	newNode := newNode(sl.arena, e.Key, e.ValueStruct, uint32(height))
	oldHeight := sl.getHeight()
	for height > oldHeight {
		// CAS to update the height of the skiplist.
		if atomic.CompareAndSwapUint32(&sl.height, oldHeight, height) {
			break
		}
		oldHeight = sl.getHeight()
	}

	newNodeOffset := getNodeOffset(sl.arena, newNode)
	for i := 0; i < int(height); i++ {
		for {
			// CAS is no need here because the newNode is not visible to other goroutines.
			newNode.next[i] = next[i]
			prevNode := getNode(sl.arena, prev[i])
			if prevNode == nil {
				// Height exceeds the old height of the skiplist.
				prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, sl.headOffset)
				prevNode = getNode(sl.arena, prev[i])
			}
			if prevNode.casNextOffset(i, next[i], newNodeOffset) {
				break
			} else {
				// Recompute the prev and next.
				prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, prev[i])
			}
		}
	}

}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// Todo with probability
func randomHeight() (height uint32) {
	height = 1
	for height < maxHeight && FastRand() <= heightIncrease {
		height++
	}
	return height
}

// Search searches the key in the skiplist.
// Only key is found, the value is returned, error is nil.
func (sl *SkipList) Search(key []byte) (vs util.ValueStruct, err error) {
	prev := sl.headOffset
	for i := int(sl.height) - 1; i >= 0; i-- {
		prev, _ = sl.findSpliceForLevel(i, key, prev)
	}
	node := getNode(sl.arena, prev)
	if bytes.Equal(node.key(sl.arena), key) {
		vs = node.vs(sl.arena)
	} else {
		err = ErrKeyNotFound
	}
	return vs, err
}

func (sl *SkipList) MemSize() uint32 {
	return sl.arena.Size()
}

type SkipListIterator struct {
	sl *SkipList
	nd *node
}

func (sl *SkipList) NewIterator() util.Iterator {
	return &SkipListIterator{
		sl: sl,
	}
}

func (s *SkipListIterator) Valid() bool {
	return s.nd != nil
}

func (s *SkipListIterator) Item() util.Item {
	return &util.Entry{
		Key:         s.Key(),
		ValueStruct: s.Value(),
	}
}

func (s *SkipListIterator) Key() []byte {
	return s.nd.key(s.sl.arena)
}

func (s *SkipListIterator) Value() util.ValueStruct {
	return s.nd.vs(s.sl.arena)
}

func (s *SkipListIterator) Next() {
	if s.nd != nil {
		s.nd = getNode(s.sl.arena, s.nd.next[0])
	}
}

func (s *SkipListIterator) Prev() {
	if s.nd == nil {
		return
	}
	prev := s.sl.headOffset
	for i := int(s.sl.height) - 1; i >= 0; i-- {
		prev, _ = s.sl.findSpliceForLevel(i, s.nd.key(s.sl.arena), prev)
	}
	s.nd = getNode(s.sl.arena, prev)
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

// Seek moves the iterator to the first entry with a key >= target.
func (s *SkipListIterator) Seek(key []byte) {
	prev := s.sl.headOffset
	for i := int(s.sl.height) - 1; i >= 0; i-- {
		prev, _ = s.sl.findSpliceForLevel(i, key, prev)
	}
	s.nd = getNode(s.sl.arena, prev)
	if bytes.Equal(s.Key(), key) {
		return
	} else {
		s.Next()
	}

}

// SeekForPrev moves the iterator to the last entry with a key <= target.
func (s *SkipListIterator) SeekForPrev(key []byte) {
	prev := s.sl.headOffset
	for i := int(s.sl.height) - 1; i >= 0; i-- {
		prev, _ = s.sl.findSpliceForLevel(i, key, prev)
	}
	if prev == s.sl.headOffset {
		s.nd = nil
		return
	}
	s.nd = getNode(s.sl.arena, prev)
	if bytes.Compare(s.Key(), key) > 0 {
		s.Prev()
	}
}

func (s *SkipListIterator) SeekToFirst() {
	s.nd = s.sl.getNext(getNode(s.sl.arena, s.sl.headOffset), 0)
}

func (s *SkipListIterator) SeekToLast() {
	prev := s.sl.headOffset
	for i := int(s.sl.height) - 1; i >= 0; i-- {
		prev, _ = s.sl.findSpliceForLevel(i, nil, prev)
	}
	s.nd = getNode(s.sl.arena, prev)
}

func (s *SkipListIterator) Close() error {
	return nil
}
