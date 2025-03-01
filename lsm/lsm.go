package lsm

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Zaire404/InfiniDB/util"
)

type LSM struct {
	memTable     *MemTable
	immutables   []*MemTable
	levelManager levelManager
	closer       *util.Closer
	opt          *Options
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{opt: opt,
		memTable:     newMemTable(),
		levelManager: *newLevelManager(opt),
		closer:       util.NewCloser(1),
	}
	return lsm
}

func (lsm *LSM) Set(entry *util.Entry) error {
	lsm.closer.AddRunning(1)
	defer lsm.closer.Done()
	if lsm.memTable.Size() >= lsm.opt.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		lsm.memTable = newMemTable()
	}
	lsm.memTable.set(entry)

	for _, immutable := range lsm.immutables {
		if err := lsm.levelManager.flush(immutable); err != nil {
			return err
		}
	}

	// free immutables
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*MemTable, 0)
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*util.Entry, error) {
	entry, err := lsm.memTable.get(key)
	if err == nil {
		return entry, nil
	}
	for _, im := range lsm.immutables {
		entry, err = im.get(key)
		if err == nil {
			return entry, nil
		}
	}
	return lsm.levelManager.Get(key)
}

func (lsm *LSM) Close() {

}

func (lsm *LSM) StartCompact() {
	lsm.closer.AddRunning(int(lsm.opt.CompactThreadCount))
	for i := 0; i < int(lsm.opt.CompactThreadCount); i++ {
		go lsm.levelManager.runCompactor(i, lsm.closer)
	}
}

func (lsm *LSM) AutoDelete() {
	for {
		select {
		case <-lsm.closer.HasBeenClosed():
			return
		case <-time.After(10 * time.Second):
			files, err := os.ReadDir(lsm.opt.WorkDir)
			if err != nil {
				continue
			}
			for _, file := range files {
				if strings.HasSuffix(file.Name(), ".del") {
					os.Remove(filepath.Join(lsm.opt.WorkDir, file.Name()))
				}
			}
		}
	}
}
