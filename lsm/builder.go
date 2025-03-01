package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/negrel/assert"
)

type tableBuilder struct {
	keyHashList []uint32 // hash list of keys
	curBlock    *block
	blocks      []*block
	indexTable  *proto.IndexTable
	size        uint32
	opt         Options
}

type block struct {
	baseKey        []byte
	arena          *util.Arena
	entryOffsets   []uint32
	entryEndOffset uint32
}

type header struct {
	overlap uint16
	diff    uint16
}

func newBlock(blockSize uint32) *block {
	return &block{
		arena: util.NewArena(blockSize),
	}
}

func newTableBuilder(opt Options) *tableBuilder {
	return &tableBuilder{
		opt:        opt,
		curBlock:   newBlock(opt.BlockSize),
		indexTable: &proto.IndexTable{},
	}
}

func (tb *tableBuilder) Empty() bool {
	return len(tb.keyHashList) == 0
}

func (tb *tableBuilder) Close() {

}

func (tb *tableBuilder) add(e *util.Entry) {
	if !tb.curBlock.canAppendEntry(e, tb.opt.BlockSize) {
		tb.commitCurBlock()
	}
	tb.keyHashList = append(tb.keyHashList, util.Hash(e.Key))
	tb.curBlock.add(e)

}

// commitCurBlock commits the current data block to the tableBuilder.
func (tb *tableBuilder) commitCurBlock() {
	if tb.curBlock == nil {
		tb.curBlock = newBlock(tb.opt.BlockSize)
		return
	}
	if tb.curBlock.arena.Size() == 1 {
		// Only the last commit caused by a flush is likely to enter this condition
		return
	}

	tb.curBlock.entryEndOffset = tb.curBlock.arena.Size()
	// Fill the remaining part of the data block format:
	// - [entryOffsets]
	// - [entryOffsetsLen]
	// - [Checksum]
	// - [ChecksumLen]
	tb.curBlock.pushBack(util.Uint32SliceToBytes(tb.curBlock.entryOffsets))
	entryOffsetsLen := len(tb.curBlock.entryOffsets)
	tb.indexTable.KeyCount += uint32(entryOffsetsLen)
	tb.curBlock.pushBack(util.Uint32ToBytes(uint32(entryOffsetsLen)))
	checksum := util.Uint32ToBytes(tb.curBlock.arena.Checksum())
	tb.curBlock.pushBack(checksum)
	tb.curBlock.pushBack(util.Uint32ToBytes(uint32(len(checksum))))
	tb.blocks = append(tb.blocks, tb.curBlock)
	tb.size += tb.curBlock.arena.Size()
	tb.curBlock = newBlock(tb.opt.BlockSize)
}

func (tb *tableBuilder) flush(lm *levelManager, tablePath string) (*Table, error) {
	tb.commitCurBlock()
	tb.buildIndexTable()
	indexData, err := proto.Marshal(tb.indexTable)
	if err != nil {
		return nil, err
	}
	indexDataLen := len(indexData)
	indexChecksum := util.Uint32ToBytes(util.Checksum(indexData))
	indexChecksumLen := len(indexChecksum)
	// already has data block size
	tb.size += uint32(indexChecksumLen) + uint32(indexDataLen) + 4 + 4

	fid, err := util.GetFIDByPath(tablePath)
	if err != nil {
		return nil, err
	}
	t := &Table{
		lm:  lm,
		fid: fid,
	}
	t.sst, err = file.OpenSSTable(&file.Options{
		FID:      fid,
		FilePath: tablePath,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(tb.size),
	})
	if err != nil {
		return nil, err
	}
	dst, err := t.sst.Bytes(0, int(tb.size))
	if err != nil {
		return nil, err
	}
	written := tb.buildTable(dst, indexData, indexChecksum)
	assert.Equal(written, int(tb.size), "written size error")
	return t, nil
}

// buildTable builds the table in dst(mmap file).
func (tb *tableBuilder) buildTable(dst []byte, indexData []byte, indexChecksum []byte) int {
	assert.Equal(len(dst), int(tb.size), "dst size error")
	var written int
	for _, block := range tb.blocks {
		written += copy(dst[written:], block.arena.Get(0, block.arena.Size()))
	}
	written += copy(dst[written:], indexData)
	written += copy(dst[written:], util.Uint32ToBytes(uint32(len(indexData))))
	written += copy(dst[written:], indexChecksum)
	written += copy(dst[written:], util.Uint32ToBytes(uint32(len(indexChecksum))))
	return written
}

func (tb *tableBuilder) buildIndexTable() {
	bf := util.NewBloomFilter(tb.keyHashList, tb.opt.BloomFilterFalsePositiveRate)
	tb.indexTable.BloomFilter = bf.Bytes()
	tb.buildBlockOffsets()
}

func (tb *tableBuilder) buildBlockOffsets() {
	var blockOffsets []*proto.BlockOffset
	var offset uint32
	for _, b := range tb.blocks {
		blockOffsets = append(blockOffsets, &proto.BlockOffset{
			Key:    b.baseKey,
			Offset: offset,
			Len:    b.arena.Size(),
		})
		offset += b.arena.Size()
	}
	tb.indexTable.Offsets = blockOffsets

}

func (tb *tableBuilder) IsReachedCapacity() bool {
	return tb.size >= tb.opt.SSTableSize
}

func (block *block) canAppendEntry(e *util.Entry, maxSize uint32) bool {
	estimatedSize := int(block.arena.Size()) + // current size
		4 + // header
		int(len(e.Key)) + // key
		int(e.ValueStruct.EncodedSize()) + // value
		(len(block.entryOffsets)+1)*4 + // entryOffsets
		4 + // entryOffsetsLen
		4 + // Checksum
		4 // ChecksumLen
	return estimatedSize <= int(maxSize)
}

func (block *block) add(e *util.Entry) {
	var diffKey []byte
	if len(block.baseKey) == 0 {
		block.baseKey = e.Key
		diffKey = e.Key
	} else {
		diffKey = util.DiffKey(block.baseKey, e.Key)
	}
	overlap := len(e.Key) - len(diffKey)
	diff := len(diffKey)
	if overlap > math.MaxUint16 || diff > math.MaxUint16 {
		panic("key length too long")
	}
	h := header{
		overlap: uint16(overlap),
		diff:    uint16(diff),
	}

	// write the KV to the block
	// - [header]
	// 		- [overlap]
	// 		- [diff]
	// - [diffKey]
	// - [value]
	// Impossible for multiple threads to write to a block
	block.entryOffsets = append(block.entryOffsets, block.arena.Size())
	block.pushBack(h.encode())
	block.pushBack(diffKey)
	encodeValue := make([]byte, e.ValueStruct.EncodedSize())
	e.ValueStruct.EncodeValue(encodeValue)
	block.pushBack(encodeValue)
}

// pushBack appends data to the block's arena.
func (block *block) pushBack(data []byte) {
	need := len(data)
	offset := block.arena.Allocate(uint32(need))
	l := copy(block.arena.Get(offset, uint32(need)), data)
	assert.Equal(l, need, "pushBack error")
}

func (h *header) encode() []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint16(buf[:2], h.overlap)
	binary.LittleEndian.PutUint16(buf[2:], h.diff)
	return buf[:]
}

func (h *header) decode(data []byte) {
	h.overlap = binary.LittleEndian.Uint16(data[:2])
	h.diff = binary.LittleEndian.Uint16(data[2:])
}

func (b *block) RecoverFromArena() error {
	// checksumLen
	offset := b.arena.Size()
	offset -= 4
	buf := b.arena.Get(offset, 4)
	checksumLen := util.BytesToUint32(buf)

	// checksum
	offset -= checksumLen
	checksum := b.arena.Get(offset, checksumLen)
	if !util.VerifyCheckSum(b.arena.Get(0, offset), checksum) {
		return ErrChecksum
	}

	// entryOffsetsLen
	offset -= 4
	buf = b.arena.Get(offset, 4)
	entryOffsetsLen := util.BytesToUint32(buf)

	// entryOffsets
	offset -= entryOffsetsLen * 4
	buf = b.arena.Get(offset, entryOffsetsLen*4)
	b.entryOffsets = util.BytesToUint32Slice(buf)
	// the first entry offset is 1 because the arena is initialized with 1
	assert.Equal(int(b.entryOffsets[0]), 1)

	// entryEndOffset
	b.entryEndOffset = offset

	//TODO: baseKey
	return nil
}

type blockIterator struct {
	entryPos int
	err      error
	block    *block
}

func (b *block) NewIterator() util.Iterator {
	return &blockIterator{
		block:    b,
		entryPos: 0,
		err:      nil,
	}
}

func (iter *blockIterator) Next() {
	iter.entryPos++
	if iter.entryPos >= len(iter.block.entryOffsets) {
		iter.err = io.EOF
		return
	}
}

func (iter *blockIterator) Valid() bool {
	return iter.err == nil
}

func (iter *blockIterator) Rewind() {
	iter.SeekToFirst()
}

func (iter *blockIterator) SeekToFirst() {
	iter.err = nil
	iter.entryPos = 0
}

func (iter *blockIterator) SeekToLast() {
	iter.err = nil
	iter.entryPos = len(iter.block.entryOffsets) - 1
}

// Seek moves the iterator to the first entry with a key >= target
func (iter *blockIterator) Seek(key []byte) {
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Entry().Key, key) >= 0 {
			return
		}
	}
}

// Only need the entryPos to get the item
func (iter *blockIterator) Item() util.Item {
	if !iter.Valid() {
		return nil
	}
	var endOffset uint32
	if iter.entryPos+1 == len(iter.block.entryOffsets) {
		endOffset = iter.block.entryEndOffset
	} else {
		endOffset = iter.block.entryOffsets[iter.entryPos+1]
	}
	offset := iter.block.entryOffsets[iter.entryPos]
	//header
	var h header
	h.decode(iter.block.arena.Get(offset, 4))
	// diffKey
	offset += 4
	diffKey := iter.block.arena.Get(offset, uint32(h.diff))
	// value
	offset += uint32(h.diff)
	value := iter.block.arena.Get(offset, endOffset-offset)
	var vs util.ValueStruct
	vs.DecodeValue(value)
	return &util.Entry{
		Key:         util.RecoverKey(iter.block.baseKey, diffKey, h.overlap),
		ValueStruct: vs,
	}
}

func (iter *blockIterator) Close() error {
	return nil
}
