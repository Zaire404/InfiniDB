package lsm

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/skl"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

const WalFileExt string = ".wal"

type MemTable struct {
	lsm *LSM
	sl  *skl.SkipList
	wal *file.WalFile
	ref uint32
}

// openMemTable opens a memtable from the given file ID of wal file.
func (lsm *LSM) openMemTable(fid uint64) (*MemTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.opt.MemTableSize),
		FID:      fid,
		FilePath: util.WalFilePath(lsm.opt.WorkDir, fid),
	}
	s := skl.NewSkipList(1 << 20)
	m := &MemTable{
		sl:  s,
		lsm: lsm,
	}
	m.wal = file.OpenWalFile(fileOpt)
	if err := m.updateSkipList(); err != nil {
		panic(err)
	}
	return m, nil
}

func (lsm *LSM) newMemTable() *MemTable {
	newFid := atomic.AddUint64(&(lsm.levelManager.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.opt.MemTableSize),
		FID:      newFid,
		FilePath: util.WalFilePath(lsm.opt.WorkDir, newFid),
	}
	return &MemTable{
		wal: file.OpenWalFile(fileOpt),
		sl:  skl.NewSkipList(100000000),
		lsm: lsm,
	}
}

func (m *MemTable) Size() uint32 {
	return m.sl.MemSize()
}

func (m *MemTable) set(entry *util.Entry) error {
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	m.sl.Add(entry)
	return nil
}

func (m *MemTable) get(key []byte) (*util.Entry, error) {
	vs, err := m.sl.Search(key)
	return &util.Entry{Key: key, ValueStruct: vs}, err
}

// updateSkipList updates the SkipList by replaying the WAL
func (m *MemTable) updateSkipList() error {
	// Check if WAL or SkipList is nil
	if m.wal == nil || m.sl == nil {
		return nil
	}

	// Iterate over the WAL and replay entries into the SkipList
	endOffset, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.opt))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}

	// Truncate the WAL to the end offset
	return m.wal.Truncate(int64(endOffset))
}

// replayFunction returns a function that replays entries into the SkipList
func (m *MemTable) replayFunction(opt *Options) func(*util.Entry, *util.ValuePtr) error {
	return func(e *util.Entry, _ *util.ValuePtr) error {
		m.sl.Add(e)
		return nil
	}
}

func (m *MemTable) close() error {
	return m.wal.Close()
}
