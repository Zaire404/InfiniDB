package lsm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/negrel/assert"
)

type Table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
}

func openTable(lm *levelManager, tablePath string, builder *tableBuilder) (*Table, error) {
	var t *Table
	var err error
	if builder != nil {
		t, err = builder.flush(lm, tablePath)
		if err != nil {
			return nil, err
		}
	} else {
		sst, err := file.OpenSSTable(&file.Options{
			FilePath: tablePath,
			MaxSize:  int(lm.opt.SSTableSize),
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
		})
		if err != nil {
			return nil, err
		}
		fid, err := util.GetFIDByPath(tablePath)
		if err != nil {
			return nil, err
		}
		t = &Table{
			sst: sst,
			lm:  lm,
			fid: fid,
		}
	}
	if err := t.sst.Init(); err != nil {
		return nil, err
	}
	iter := t.NewIterator(&util.Options{})
	iter.SeekToLast()
	t.sst.SetMaxKey(iter.Item().Entry().Key)
	return t, nil
}

func (t *Table) Search(key []byte) (*util.Entry, error) {
	bf := util.BloomFilter(t.sst.IndexTable().GetBloomFilter())
	if t.sst.HasBloomFilter() && !bf.MayContainKey(key) {
		return nil, ErrKeyNotFound
	}
	iter := t.NewIterator(&util.Options{})
	iter.Seek(key)
	if iter.Valid() {
		entryFound := iter.Item().Entry()
		keyFound := entryFound.Key
		if bytes.Equal(key, keyFound) {
			return entryFound, nil
		}
	}
	return nil, ErrKeyNotFound
}

// TODO: use sync.Pool to optimize the performance.
type tableIterator struct {
	opt       *util.Options
	table     *Table
	blockPos  int
	blockIter util.Iterator
	err       error
}

func (t *Table) RecoverBlockFromBlockOffset(bo *proto.BlockOffset) *block {
	data, err := t.sst.Bytes(int(bo.GetOffset()), int(bo.GetLen()))
	if err != nil {
		panic(err)
	}
	block := &block{
		baseKey: bo.GetKey(),
		arena:   util.RecoverArena(data, bo.GetLen()),
	}
	err = block.RecoverFromArena()
	if err != nil {
		panic(err)
	}
	return block
}
func (t *Table) NewIterator(opt *util.Options) util.Iterator {
	blockOffsets := t.sst.IndexTable().GetOffsets()
	assert.Greater(len(blockOffsets), 0, fmt.Sprintf("table %d has no block", t.fid))
	firstBlockOffset := blockOffsets[0]

	block := t.RecoverBlockFromBlockOffset(firstBlockOffset)

	return &tableIterator{
		opt:       opt,
		table:     t,
		blockPos:  0,
		blockIter: block.NewIterator(),
	}
}

func (iter *tableIterator) Next() {
	if !iter.Valid() {
		return
	}

	iter.blockIter.Next()
	if iter.blockIter.Valid() {
		return
	}
	iter.SeekToNthBlock(iter.blockPos + 1)
}

func (iter *tableIterator) Valid() bool {
	return iter.err == nil && iter.blockIter.Valid()
}

func (iter *tableIterator) Rewind() {
	iter.SeekToNthBlock(0)
}

func (iter *tableIterator) SeekToNthBlock(blockPos int) {
	iter.err = nil
	if iter.blockPos == blockPos {
		iter.blockIter.Rewind()
		return
	}
	iter.blockPos = blockPos
	blockOffsets := iter.table.sst.IndexTable().GetOffsets()
	if iter.blockPos >= len(blockOffsets) {
		iter.err = io.EOF
		return
	}
	blockOffset := blockOffsets[iter.blockPos]
	block := iter.table.RecoverBlockFromBlockOffset(blockOffset)
	iter.blockIter = block.NewIterator()
}

func (iter *tableIterator) SeekToFirst() {
	iter.SeekToNthBlock(0)
}

func (iter *tableIterator) SeekToLast() {
	iter.SeekToNthBlock(len(iter.table.sst.IndexTable().GetOffsets()) - 1)
	iter.blockIter.SeekToLast()
}

// Seek moves the iterator to the first entry with a key >= target
func (iter *tableIterator) Seek(key []byte) {
	iter.err = nil
	blockOffsets := iter.table.sst.IndexTable().GetOffsets()
	index := sort.Search(len(blockOffsets), func(i int) bool {
		return bytes.Compare(blockOffsets[i].GetKey(), key) > 0
	})

	index--
	if index < 0 || index >= len(blockOffsets) {
		iter.err = io.EOF
		return
	}
	iter.SeekToNthBlock(index)
	iter.blockIter.Seek(key)
}

func (iter *tableIterator) Item() util.Item {
	return iter.blockIter.Item()
}

func (iter *tableIterator) Close() error {
	return nil
}
