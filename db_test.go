package infinidb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Zaire404/InfiniDB/util"
)

var opt = DefaultOptions("./work_test")

func clearWorkDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}

func baseTest(t *testing.T, db *DB, n int) {
	for i := 0; i < n; i++ {
		err := db.Set(util.NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte("value")))
		if err != nil {
			t.Logf("Set error for key%d: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Log("set done")
	for i := 0; i < n; i++ {
		entry, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Logf("Get error for key%d: %v", i, err)
			t.Fail()
		}
		if string(entry.ValueStruct.Value) != "value" {
			t.Logf("Value mismatch for key%d: got %s, want %s", i, entry.ValueStruct.Value, "value")
			t.Fail()
		}
	}
	t.Log("get done")
}
func TestAPI(t *testing.T) {
	clearWorkDir()
	db := Open(&opt)
	defer db.Close()
	db.GC()
	entry := &util.Entry{
		Key: []byte("key"),
		ValueStruct: util.ValueStruct{
			Value:    []byte("value"),
			ExpireAt: 0,
		},
	}
	if err := db.Set(entry); err != nil {
		t.Fatal(err)
	}
	if e, err := db.Get([]byte("key")); err != nil {
		t.Fatal(err)
	} else {
		t.Log(e)
	}
	if err := db.Del([]byte("key")); err != nil {
		t.Fatal(err)
	}
}

func TestRecovery(t *testing.T) {
	clearWorkDir()
	const n = 100
	db := Open(&opt)
	baseTest(t, db, n)
	db.Close()
	db = Open(&opt)
	defer db.Close()
	for i := 0; i < n; i++ {
		entry, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Logf("Get error for key%d: %v", i, err)
			t.Fail()
		}
		if string(entry.ValueStruct.Value) != "value" {
			t.Logf("Value mismatch for key%d: got %s, want %s", i, entry.ValueStruct.Value, "value")
			t.Fail()
		}
	}
}

func BenchmarkFillRandom(b *testing.B) {
	FillRandom(b, 16, 100)
}

func FillRandom(b *testing.B, keySize int, valueSize int) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(util.GetRandomString(keySize))
		values[i] = []byte(util.GetRandomString(valueSize))
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
	FillSeq(b, 16, 100)
}

func FillSeq(b *testing.B, keySize int, valueSize int) {
	clearWorkDir()
	db := Open(&opt)
	db.GC()
	defer db.Close()

	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		numLen := keySize - len("key")
		key := fmt.Sprintf("key%0*d", numLen, i)
		keys[i] = []byte(key)
		values[i] = []byte(util.GetRandomString(valueSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Set(util.NewEntry(keys[i], values[i]))
		if err != nil {
			b.Logf("Set error for key%d: %v", i, err)
		}
	}
}
