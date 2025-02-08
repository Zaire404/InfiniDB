package lsm

import (
	"bytes"
	"sort"
	"sync/atomic"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/util"
)

type levelManager struct {
	levels []*levelController
	maxFID uint64
	opt    *Options
}

type levelController struct {
	levelNumber int
	tables      []*Table // tables should be sorted by fid in ascending order
	size        int64
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt:    opt,
		levels: make([]*levelController, opt.MaxLevelNum),
	}
	for i := 0; i < int(opt.MaxLevelNum); i++ {
		lm.levels[i] = newLevelController(i)
	}
	// TODO: load levels from manifest
	return lm
}

func newLevelController(levelNumber int) *levelController {
	return &levelController{
		levelNumber: levelNumber,
	}
}

func (lm *levelManager) flush(immutable *MemTable) error {
	nextFID := atomic.AddUint64(&lm.maxFID, 1)
	sstName := util.GenSSTName(nextFID)
	sstPath := lm.opt.WorkDir + "/" + sstName
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		builder.add(iter.Item().Entry())
	}
	table, err := openTable(lm, sstPath, builder)
	if err != nil {
		return err
	}

	// tables is sorted by fid in ascending order
	lm.levels[0].addTable(table)

	// TODO: add table to manifest
	return nil
}

func (lm *levelManager) Get(key []byte) (*util.Entry, error) {
	var entry *util.Entry
	var err error
	for i := 0; i < int(lm.opt.MaxLevelNum); i++ {
		if entry, err = lm.levels[i].Get(key); err == nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lc *levelController) addTable(table *Table) {
	lc.tables = append(lc.tables, table)
	atomic.AddInt64(&lc.size, 1)
}

func (lc levelController) Get(key []byte) (*util.Entry, error) {
	if lc.levelNumber == 0 {
		return lc.searchL0(key)
	} else {
		return lc.serachLn(key)
	}
}

func (lc *levelController) searchL0(key []byte) (*util.Entry, error) {
	// tables is sorted by fid in ascending order
	for i := len(lc.tables) - 1; i >= 0; i-- {
		if entry, err := lc.tables[i].Search(key); err == nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lc *levelController) serachLn(key []byte) (*util.Entry, error) {
	if lc.size > 0 && (bytes.Compare(key, lc.tables[0].sst.MinKey()) < 0 ||
		bytes.Compare(key, lc.tables[len(lc.tables)-1].sst.MaxKey()) > 0) {
		return nil, ErrKeyNotFound
	}

	index := sort.Search((int)(lc.size), func(i int) bool {
		return bytes.Compare(key, lc.tables[i].sst.MaxKey()) < 1
	})
	if index < int(lc.size) && bytes.Compare(key, lc.tables[index].sst.MinKey()) >= 0 {
		return lc.tables[index].Search(key)
	}

	return nil, ErrKeyNotFound
}
