package lsm

import (
	"bytes"
	"sort"
	"sync/atomic"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/util"
)

type levelManager struct {
	levels       []*levelController
	maxFID       uint64
	manifestFile *file.ManifestFile
	opt          *Options
}

type levelController struct {
	levelNumber int
	tables      []*Table // tables should be sorted by fid in ascending order
	size        int64
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt: opt,
	}
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

func newLevelController(levelNumber int) *levelController {
	return &levelController{
		levelNumber: levelNumber,
	}
}

func (lm *levelManager) loadManifest() error {
	var err error
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkDir,
	})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelController, lm.opt.LevelCount)
	for i := 0; i < int(lm.opt.LevelCount); i++ {
		lm.levels[i] = newLevelController(i)
	}
	if err := lm.manifestFile.SyncManifestWithDir(lm.opt.WorkDir); err != nil {
		return err
	}

	manifest := lm.manifestFile.GetManifest()
	for fid, tableInfo := range manifest.Tables {
		if fid > lm.maxFID {
			lm.maxFID = fid
		}
		fileName := util.GenSSTName(fid)

		t, err := openTable(lm, fileName, nil)
		if err != nil {
			return err
		}
		lm.levels[tableInfo.Level].addTable(t)
	}

	for i := 0; i < int(lm.opt.LevelCount); i++ {
		lm.levels[i].Sort()
	}
	return nil
}

func (lc *levelController) Sort() {
	if lc.levelNumber == 0 {
		// L1
		sort.Slice(lc.tables, func(i, j int) bool {
			return lc.tables[i].fid < lc.tables[j].fid
		})
	} else {
		// Ln
		sort.Slice(lc.tables, func(i, j int) bool {
			return bytes.Compare(lc.tables[i].sst.MinKey(), lc.tables[j].sst.MinKey()) < 0
		})
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

	if err := lm.manifestFile.AddTable(nextFID, 0); err != nil {
		return err
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (*util.Entry, error) {
	var entry *util.Entry
	var err error
	for i := 0; i < int(lm.opt.LevelCount); i++ {
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
