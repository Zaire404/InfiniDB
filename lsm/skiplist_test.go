package lsm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Zaire404/ZDB/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := util.NewEntry([]byte(util.GetRandomString(10)), []byte("Val1"))
	list.Add(entry1)
	vs, _ := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, vs)

	entry2 := util.NewEntry([]byte(util.GetRandomString(10)), []byte("Val2"))
	list.Add(entry2)
	vs, _ = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs)

	//Get a not exist entry
	vs, _ = list.Search([]byte(util.GetRandomString(10)))
	assert.Nil(t, vs.Value)

	//Update a entry
	entry2_new := util.NewEntry(entry1.Key, []byte("Val1+1"))
	list.Add(entry2_new)
	vs, _ = list.Search(entry2_new.Key)
	assert.Equal(t, entry2_new.Value, vs)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	// runtime.GOMAXPROCS(1)
	// b.SetParallelism(1)
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = util.GetRandomString(10), fmt.Sprintf("Val%d", i)
		entry := util.NewEntry([]byte(key), []byte(val))
		list.Add(entry)
		searchVal, _ := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 100000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Key%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(util.NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _ := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 100000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Key%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(util.NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _ := l.Search(key(i))
			require.EqualValues(b, key(i), v.Value)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & Get
	entry1 := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry1)
	vs, err := list.Search(entry1.Key)
	require.NoError(t, err)
	assert.Equal(t, entry1.Value.Value, vs.Value)

	entry2 := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry2)
	vs, err = list.Search(entry2.Key)
	require.NoError(t, err)
	assert.Equal(t, entry2.Value.Value, vs.Value)

	//Update a entry
	entry2_new := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry2_new)
	vs, err = list.Search(entry2_new.Key)
	require.NoError(t, err)
	assert.Equal(t, entry2_new.Value.Value, vs.Value)

	iter := list.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Item().Key, string(iter.Item().Value.Value))
	}
}
