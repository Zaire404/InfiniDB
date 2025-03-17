package infinidb

import (
	"fmt"
	"testing"

	"github.com/Zaire404/InfiniDB/util"
)

type BenchmarkOptions struct {
	KeySize   int
	ValueSize int
}

var benchOpts = []BenchmarkOptions{
	{KeySize: 16, ValueSize: 64},
	{KeySize: 16, ValueSize: 128},
	{KeySize: 16, ValueSize: 256},
	{KeySize: 16, ValueSize: 512},
	{KeySize: 16, ValueSize: 1024},
	{KeySize: 16, ValueSize: 2048},
	{KeySize: 16, ValueSize: 4096},
	{KeySize: 16, ValueSize: 8192},
}

func BenchmarkFillRandom(b *testing.B) {
	for _, opt := range benchOpts {
		b.Run(fmt.Sprintf("KeySize=%dbytes,ValueSize=%dbytes ", opt.KeySize, opt.ValueSize), func(b *testing.B) {
			FillRandom(b, &opt)
		})
	}
}

func FillRandom(b *testing.B, benchOpt *BenchmarkOptions) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(util.GetRandomString(benchOpt.KeySize))
		values[i] = []byte(util.GetRandomString(benchOpt.ValueSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Set(util.NewEntry(keys[i], values[i]))
		if err != nil {
			b.Logf("Set error for key%d: %v", i, err)
		}
	}
	b.StopTimer()
}

func BenchmarkFillSeq(b *testing.B) {
	for _, opt := range benchOpts {
		b.Run(fmt.Sprintf("KeySize=%dbytes,ValueSize=%dbytes ", opt.KeySize, opt.ValueSize), func(b *testing.B) {
			FillSeq(b, &opt)
		})
	}
}

func FillSeq(b *testing.B, benchOpt *BenchmarkOptions) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()

	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("%0*d", benchOpt.KeySize, i))
		values[i] = []byte(util.GetRandomString(benchOpt.ValueSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Set(util.NewEntry(keys[i], values[i]))
		if err != nil {
			b.Logf("Set error for key%d: %v", i, err)
		}
	}
	b.StopTimer()
}

func BenchmarkReadSeq(b *testing.B) {
	for _, opt := range benchOpts {
		b.Run(fmt.Sprintf("KeySize=%dbytes,ValueSize=%dbytes ", opt.KeySize, opt.ValueSize), func(b *testing.B) {
			ReadSeq(b, &opt)
		})
	}
}

func ReadSeq(b *testing.B, benchOpt *BenchmarkOptions) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()

	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("%0*d", benchOpt.KeySize, i))
		values[i] = []byte(util.GetRandomString(benchOpt.ValueSize))
	}
	// Pre-fill the database with values
	for i := 0; i < b.N; i++ {
		err := db.Set(util.NewEntry(keys[i], values[i]))
		if err != nil {
			b.Logf("Set error for key%d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i])
		if err != nil {
			b.Logf("Get error for key%d: %v", i, err)
		}
	}
	b.StopTimer()
}
func BenchmarkReadRandom(b *testing.B) {
	for _, opt := range benchOpts {
		b.Run(fmt.Sprintf("KeySize=%dbytes,ValueSize=%dbytes ", opt.KeySize, opt.ValueSize), func(b *testing.B) {
			ReadRandom(b, &opt)
		})
	}
}

func ReadRandom(b *testing.B, benchOpt *BenchmarkOptions) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()

	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(util.GetRandomString(benchOpt.KeySize))
		values[i] = []byte(util.GetRandomString(benchOpt.ValueSize))
	}

	// Pre-fill the database with values
	for i := 0; i < b.N; i++ {
		err := db.Set(util.NewEntry(keys[i], values[i]))
		if err != nil {
			b.Logf("Set error for key%d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i])
		if err != nil {
			b.Logf("Get error for key%d: %v", i, err)
		}
	}
	b.StopTimer()
}
