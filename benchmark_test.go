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

var benchOpt = &BenchmarkOptions{
	KeySize:   16,
	ValueSize: 128,
}

func BenchmarkFillRandom(b *testing.B) {
	FillRandom(b, benchOpt)
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
}

func BenchmarkFillSeq(b *testing.B) {
	FillSeq(b, benchOpt)
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
}

func BenchmarkReadSeq(b *testing.B) {
	ReadSeq(b, benchOpt)
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
}
func BenchmarkReadRandom(b *testing.B) {
	ReadRandom(b, benchOpt)
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
}
