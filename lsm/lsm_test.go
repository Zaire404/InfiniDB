package lsm

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Zaire404/InfiniDB/config"
	"github.com/Zaire404/InfiniDB/log"

	"github.com/Zaire404/InfiniDB/util"
	"github.com/stretchr/testify/require"
)

var (
	opt = &Options{
		WorkDir:                      "../work_test",
		SSTableSize:                  4096,
		MemTableSize:                 4096,
		BlockSize:                    1024,
		BloomFilterFalsePositiveRate: 0.01,
		LevelCount:                   5,
		LevelSizeMultiplier:          10,
		BaseLevelSize:                4096 << 4,
		BaseTableSize:                4096,
		CompactThreadCount:           1,
		NumLevelZeroTables:           3,
	}
)

func Init() {
	config.Init()
	log.Init()
	clearDir()
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}

func TestSet(t *testing.T) {
	Init()
	const n = 1000
	lsm := NewLSM(opt)
	lsm.Set(util.NewEntry([]byte("key000"), []byte("value")))
	for i := 1; i < n; i++ {
		err := lsm.Set(util.NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte("value")))
		if err != nil {
			t.Log(err)
		}
	}
	t.Logf("table count is %d", lsm.levelManager.levels[0].tableCount)
	for _, table := range lsm.levelManager.levels[0].tables {
		t.Logf("minKey: %s", table.sst.MinKey())
	}
}

func BenchmarkSet(b *testing.B) {
	Init()
	lsm := NewLSM(opt)
	b.N = 100000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte("value")
		err := lsm.Set(util.NewEntry(key, value))
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()
}

func TestGet(t *testing.T) {
	Init()
	const n = 1000
	lsm := NewLSM(opt)
	for i := 0; i < n; i++ {
		err := lsm.Set(util.NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte("value")))
		if err != nil {
			t.Log(err)
		}
	}
	for i := 0; i < n; i++ {
		entry, err := lsm.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Log(err)
		}
		if string(entry.ValueStruct.Value) != "value" {
			t.Errorf("expect value is value, but got %s", entry.ValueStruct.Value)
		}
	}
	for i := 0; i < n; i++ {
		_, err := lsm.Get([]byte(fmt.Sprintf("nokey%d", i)))
		require.Error(t, err)
	}
}

func Benchmark_Compact(b *testing.B) {
	Init()
	lsm := NewLSM(opt)
	b.N = 1000
	// 启动压缩操作
	go lsm.StartCompact()
	go lsm.AutoDelete()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte("value")
		err := lsm.Set(util.NewEntry(key, value))
		if err != nil {
			b.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte("valuediff")
		err := lsm.Set(util.NewEntry(key, value))
		if err != nil {
			b.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < int(opt.LevelCount); i++ {
		fmt.Printf("level %d\n: ", i)
		for _, table := range lsm.levelManager.levels[i].tables {
			fmt.Printf("id: %d, minkey: %s, maxkey: %s\n", table.ID(), table.MinKey(), table.MaxKey())
		}
		fmt.Println()
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		entry, err := lsm.Get(key)
		if err != nil {
			fmt.Print(lsm.levelManager.String())
			b.Errorf("%s not found", key)
		}
		if string(entry.ValueStruct.Value) != "valuediff" {
			b.Errorf("expect value is valuediff, but got %s", entry.ValueStruct.Value)
		}
	}
	b.StopTimer()
}

func TestRecovery(t *testing.T) {
	Init()
	lsm := NewLSM(opt)
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte("value")
		err := lsm.Set(util.NewEntry(key, value))
		if err != nil {
			t.Error(err)
		}
	}
	lsm.Close()
	lsm = NewLSM(opt)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Errorf("%s not found", key)
		}
	}
}
