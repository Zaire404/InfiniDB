package lsm

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Zaire404/InfiniDB/log"
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
	lsm := &LSM{
		opt:          opt,
		levelManager: *newLevelManager(opt),
		closer:       util.NewCloser(1),
	}
	// recovery to get immutables
	lsm.immutables = lsm.recovery()
	lsm.memTable = lsm.newMemTable()
	return lsm
}

func (lsm *LSM) recovery() []*MemTable {
	// Read all files in the working directory
	files, err := os.ReadDir(lsm.opt.WorkDir)
	if err != nil {
		log.Logger.Errorf("recovery error: %v", err)
		return nil
	}

	var immutables []*MemTable
	var maxFID uint64
	var fids []uint64

	// Iterate over the files and collect WAL file IDs
	for _, file := range files {
		if strings.HasSuffix(file.Name(), WalFileExt) {
			fid, err := strconv.ParseUint(file.Name()[:len(file.Name())-len(WalFileExt)], 10, 64)
			if err != nil {
				log.Logger.Errorf("recovery error: %v", err)
				return nil
			}
			if fid > maxFID {
				maxFID = fid
			}
			fids = append(fids, fid)
		}
	}

	// Update the maximum file ID in the level manager
	lsm.levelManager.maxFID = maxFID

	// Sort the file IDs in ascending order
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	// Open MemTables for each file ID and collect non-empty ones
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		if err != nil {
			panic(err)
		}
		if mt.sl.MemSize() == 0 {
			continue
		}
		immutables = append(immutables, mt)
	}

	return immutables
}

func (lsm *LSM) Set(entry *util.Entry) error {
	lsm.closer.AddRunning(1)
	defer lsm.closer.Done()
	if lsm.memTable.Size() >= lsm.opt.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		lsm.memTable = lsm.newMemTable()
	}
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}

	for _, immutable := range lsm.immutables {
		if err := lsm.levelManager.flush(immutable); err != nil {
			return err
		}
		if err := immutable.close(); err != nil {
			panic(err)
		}
	}

	// free immutables
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*MemTable, 0)
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*util.Entry, error) {
	lsm.closer.AddRunning(1)
	defer lsm.closer.Done()
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

func (lsm *LSM) Close() error {
	lsm.closer.Close()
	if err := lsm.levelManager.close(); err != nil {
		return err
	}
	return nil
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
		case <-time.After(30 * time.Second):
			lsm.closer.AddRunning(1)
			defer lsm.closer.Done()
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
