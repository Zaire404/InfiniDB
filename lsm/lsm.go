package lsm

import (
	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/util"
)

type LSM struct {
	memTable     *MemTable
	immutables   []*MemTable
	levelManager levelManager
	opt          *Options
	maxMemFID    uint64
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{opt: opt,
		memTable:     newMemTable(),
		levelManager: *newLevelManager(opt),
	}
	return lsm
}

func (lsm *LSM) Set(entry *util.Entry) error {
	if lsm.memTable.Size() >= lsm.opt.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		lsm.memTable = newMemTable()
	}
	lsm.memTable.set(entry)

	// TODO: flush immutables to sstable
	for _, immutable := range lsm.immutables {
		if err := lsm.levelManager.flush(immutable); err != nil {
			return err
		}
	}
	//释放immutable表
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*MemTable, 0)
	}
	return nil
}

func (lsm *LSM) Get(key []byte) ([]byte, error) {
	entry, err := lsm.memTable.get(key)
	if err == nil {
		return entry.ValueStruct.Value, nil
	}
	for _, im := range lsm.immutables {
		entry, err = im.get(key)
		if err == nil {
			return entry.ValueStruct.Value, nil
		}
	}
	// TODO: search sstables

	return nil, ErrKeyNotFound
}
