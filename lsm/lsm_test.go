package lsm

import (
	"fmt"
	"testing"

	"github.com/Zaire404/InfiniDB/util"
)

var (
	// 初始化opt
	opt = &Options{
		WorkDir:                      "../work_test",
		SSTableSize:                  4096,
		MemTableSize:                 4096,
		BlockSize:                    1024,
		BloomFilterFalsePositiveRate: 0,
		MaxLevelNum:                  5,
	}
)

func TestSet(t *testing.T) {
	const n = 1000
	lsm := NewLSM(opt)
	lsm.memTable = newMemTable()
	lsm.Set(util.NewEntry([]byte("key000"), []byte("value")))
	for i := 1; i < n; i++ {
		err := lsm.Set(util.NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte("value")))
		if err != nil {
			t.Log(err)
		}
	}
	t.Logf("table count is %d", lsm.levelManager.levels[0].size)
	for _, table := range lsm.levelManager.levels[0].tables {
		t.Logf("minKey: %s", table.sst.MinKey())
	}
}
