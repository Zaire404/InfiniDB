package lsm

import (
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
	tables      []*Table
	size        uint64
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
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		builder.add(iter.Item())
	}
	table, err := openTable(lm, sstPath, builder)
	if err != nil {
		return err
	}

	lm.levels[0].addTable(table)

	// TODO: add table to manifest
	return nil
}

func (lm *levelManager) Get(key []byte) (*util.Entry, error) {
	var entry *util.Entry
	var err error
	entry, err = lm.levels[0].Get(key)
	return entry, err
}

func (lc *levelController) addTable(table *Table) {
	lc.tables = append(lc.tables, table)
	atomic.AddUint64(&lc.size, 1)
}

func (lc levelController) Get(key []byte) (*util.Entry, error) {
	if lc.levelNumber == 0 {
		return lc.searchL0(key)
	} else {
		return lc.serachLn(key)
	}
}

func (lc *levelController) searchL0(key []byte) (*util.Entry, error) {
	for _, table := range lc.tables {
		if entry, err := table.Search(key); err == nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lc *levelController) serachLn(key []byte) (*util.Entry, error) {
	return nil, nil
}
