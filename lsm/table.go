package lsm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/negrel/assert"
	"github.com/pkg/errors"
)

type Table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32
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
		// fmt.Printf("%s not found in bloom filter\n", key)
		return nil, ErrKeyNotFound
	}
	iter := t.NewIterator(&util.Options{})
	iter.Seek(key)
	if iter.Valid() {
		entryFound := iter.Item().Entry()
		keyFound := entryFound.Key
		// fmt.Println("keyFound: ", string(keyFound))
		if bytes.Equal(key, keyFound) {
			return entryFound, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (t *Table) Size() int64 {
	return t.sst.Size()
}

func (t *Table) ID() uint64 {
	return t.fid
}

func (t *Table) MinKey() []byte {
	return t.sst.MinKey()
}

func (t *Table) MaxKey() []byte {
	return t.sst.MaxKey()
}

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *Table) DecrRef() error {
	atomic.AddInt32(&t.ref, -1)
	// TODO: delete from cache
	return nil
}

func (t *Table) Close() error {
	return nil
}

func DecrRefs(tables []*Table) error {
	for _, t := range tables {
		err := t.DecrRef()
		if err != nil {
			return err
		}
	}
	return nil
}

func tablesToString(tables []*Table) []string {
	var s []string
	s = append(s, "\"")
	for _, t := range tables {
		s = append(s, fmt.Sprintf("%d", t.fid))
	}
	s = append(s, "\"")
	return s
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

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	left  MergeNode
	right MergeNode
	small *MergeNode

	curKey  []byte
	reverse bool
}

type MergeNode struct {
	valid bool
	key   []byte
	iter  util.Iterator

	// The two iterators are type asserted from `util.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []util.Iterator, reverse bool) util.Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]util.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}

func (mi *MergeIterator) bigger() *MergeNode {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *MergeIterator) setCurrent() {
	mi.curKey = append(mi.curKey[:0], mi.small.key...)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) SeekToFirst() {

}

func (mi *MergeIterator) SeekToLast() {

}

// Seek brings us to element with key >= given key.
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

// Key returns the key associated with the current iterator.
func (mi *MergeIterator) Key() []byte {
	return mi.small.key
}

// Value returns the value associated with the iterator.
func (mi *MergeIterator) Value() util.ValueStruct {
	return mi.small.iter.Item().Entry().ValueStruct
}

// Item returns the item associated with the iterator.
func (mi *MergeIterator) Item() util.Item {
	return mi.small.iter.Item()
}

// Close implements util.Iterator.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return errors.Wrap(err1, "MergeIterator")
	}
	return errors.Wrap(err2, "MergeIterator")
}

func (n *MergeNode) setIterator(iter util.Iterator) {
	n.iter = iter
	// It's okay if the type assertion below fails and n.merge/n.concat are set to nil.
	// We handle the nil values of merge and concat in all the methods.
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

func (n *MergeNode) setKey() {
	switch {
	case n.merge != nil:
		n.valid = n.merge.small.valid
		if n.valid {
			n.key = n.merge.small.key
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.key = n.concat.Key()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.key = n.iter.Item().Entry().Key
		}
	}
}

func (n *MergeNode) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *MergeNode) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *MergeNode) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := bytes.Compare(mi.small.key, mi.bigger().key)
	switch {
	case cmp == 0: // Both the keys are equal.
		// In case of same keys, move the right iterator ahead.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0: // Small is less than bigger().
		if mi.reverse {
			mi.swapSmall()
		} else {
			// we don't need to do anything. Small already points to the smallest.
		}
		return
	default: // bigger() is less than small.
		if mi.reverse {
			// Do nothing since we're iterating in reverse. Small currently points to
			// the bigger key and that'ci okay in reverse iteration.
		} else {
			mi.swapSmall()
		}
		return
	}
}

// ConcatIterator concatenates the sequences defined by several iterators.
// It only works with TableIterators.
type ConcatIterator struct {
	idx     int // Which iterator is active now.
	cur     util.Iterator
	iters   []util.Iterator // Corresponds to tables.
	tables  []*Table        // Disregarding reversed, this is in ascending order.
	options *util.Options
}

// NewConcatIterator creates a new concatenated iterator
func NewConcatIterator(tables []*Table, opt *util.Options) *ConcatIterator {
	iters := make([]util.Iterator, len(tables))
	for i := 0; i < len(tables); i++ {
		// Increment the reference count. Since, we're not creating the iterator right now.
		// Here, We'll hold the reference of the tables, till the lifecycle of the iterator.
		tables[i].IncrRef()

		// Save cycles by not initializing the iterators until needed.
		// iters[i] = tbls[i].NewIterator(reversed)
	}
	return &ConcatIterator{
		options: opt,
		iters:   iters,
		tables:  tables,
		idx:     -1, // Not really necessary because ci.Valid()=false, but good to have.
	}
}

func (ci *ConcatIterator) setIdx(idx int) {
	ci.idx = idx
	if idx < 0 || idx >= len(ci.iters) {
		ci.cur = nil
		return
	}
	// lazy loading
	if ci.iters[idx] == nil {
		ci.iters[idx] = ci.tables[idx].NewIterator(ci.options)
	}
	ci.cur = ci.iters[ci.idx]
}

func (ci *ConcatIterator) Rewind() {
	if len(ci.iters) == 0 {
		return
	}
	if ci.options.IsAsc == true {
		ci.setIdx(0)
	} else {
		ci.setIdx(len(ci.iters) - 1)
	}
	ci.cur.Rewind()
}

func (ci *ConcatIterator) SeekToFirst() {
}

func (ci *ConcatIterator) SeekToLast() {
}

func (ci *ConcatIterator) Item() util.Item {
	return ci.cur.Item()
}

func (ci *ConcatIterator) Valid() bool {
	return ci.cur != nil && ci.cur.Valid()
}

func (ci *ConcatIterator) Key() []byte {
	return ci.cur.Item().Entry().Key
}

func (ci *ConcatIterator) Value() util.ValueStruct {
	return ci.cur.Item().Entry().ValueStruct
}

// Seek brings us to element >= key if options.IsAsc is false. Otherwise, <= key.
func (ci *ConcatIterator) Seek(key []byte) {
	var idx int
	if ci.options.IsAsc {
		idx = sort.Search(len(ci.tables), func(i int) bool {
			return bytes.Compare(ci.tables[i].sst.MaxKey(), key) >= 0
		})
	} else {
		n := len(ci.tables)
		idx = n - sort.Search(n, func(i int) bool {
			return bytes.Compare(ci.tables[n-i-1].sst.MinKey(), key) <= 0
		})
	}
	if idx >= len(ci.tables) || idx < 0 {
		ci.setIdx(-1)
		return
	}

	ci.setIdx(idx)
	ci.cur.Seek(key)
}

// Next advances our concat iterator.
func (ci *ConcatIterator) Next() {
	ci.cur.Next()
	if ci.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if ci.options.IsAsc == true {
			ci.setIdx(ci.idx + 1)
		} else {
			ci.setIdx(ci.idx - 1)
		}
		if ci.cur == nil {
			// End of list. Valid will become false.
			return
		}
		ci.cur.Rewind()
		if ci.cur.Valid() {
			break
		}
	}
}

func (ci *ConcatIterator) Close() error {
	for _, t := range ci.tables {
		// DeReference the tables while closing the iterator.
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	for _, it := range ci.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return errors.Wrap(err, "ConcatIterator")
		}
	}
	return nil
}
