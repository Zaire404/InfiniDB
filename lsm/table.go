package lsm

import (
	"os"

	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/util"
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
	return t, nil
}

func (t *Table) Search(key []byte) (*util.Entry, error) {
	return nil, nil
}
