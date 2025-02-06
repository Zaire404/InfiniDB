package lsm

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Zaire404/InfiniDB/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Draw plot Skiplist, align represents align the same node in different level
func (s *SkipList) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := getNode(s.arena, s.headOffset)
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNext(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.vs(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := strings.Repeat("-", len(ele))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}
func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(1000)

	//Put & Get
	entry1 := util.NewEntry([]byte(util.GetRandomString(10)), []byte("Val1"))
	list.Add(entry1)
	vs, _ := list.Search(entry1.Key)
	assert.Equal(t, entry1.ValueStruct, vs)

	entry2 := util.NewEntry([]byte(util.GetRandomString(10)), []byte("Val2"))
	list.Add(entry2)
	vs, _ = list.Search(entry2.Key)
	assert.Equal(t, entry2.ValueStruct, vs)

	//Get a not exist entry
	vs, _ = list.Search([]byte(util.GetRandomString(10)))
	assert.Nil(t, vs.Value)

	//Update a entry
	entry2_new := util.NewEntry(entry1.Key, []byte("Val1+1"))
	list.Add(entry2_new)
	vs, _ = list.Search(entry2_new.Key)
	assert.Equal(t, entry2_new.ValueStruct, vs)

	// Test duplicate keys
	entry3 := util.NewEntry([]byte("DuplicateKey"), []byte("Value1"))
	list.Add(entry3)
	entry4 := util.NewEntry([]byte("DuplicateKey"), []byte("Value2"))
	list.Add(entry4)
	vs, _ = list.Search([]byte("DuplicateKey"))
	assert.Equal(t, entry4.ValueStruct, vs)
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

	// empty case
	iter := list.NewSkipListIterator()
	assert.False(t, iter.Valid())
	iter.Rewind()
	assert.False(t, iter.Valid())
	iter.Seek([]byte("key"))
	assert.False(t, iter.Valid())
	iter.SeekForPrev([]byte("key"))
	assert.False(t, iter.Valid())

	//Put & Get
	entry1 := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry1)
	vs, err := list.Search(entry1.Key)
	require.NoError(t, err)
	assert.Equal(t, entry1.ValueStruct.Value, vs.Value)

	entry2 := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry2)
	vs, err = list.Search(entry2.Key)
	require.NoError(t, err)
	assert.Equal(t, entry2.ValueStruct.Value, vs.Value)

	//Update a entry
	entry2_new := util.NewEntry([]byte(util.GetRandomString(10)), []byte(util.GetRandomString(10)))
	list.Add(entry2_new)
	vs, err = list.Search(entry2_new.Key)
	require.NoError(t, err)
	assert.Equal(t, entry2_new.ValueStruct.Value, vs.Value)

	list.Draw(false)

	// Test iterator over multiple entries
	iter.Rewind()
	assert.True(t, iter.Valid())
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	assert.Equal(t, 3, count) // We added 3 entries

	// Test iterator seek
	iter.Seek(entry1.Key)
	assert.True(t, iter.Valid())
	assert.Equal(t, entry1.Key, iter.Item().Key)

	iter.Seek(entry2.Key)
	assert.True(t, iter.Valid())
	assert.Equal(t, entry2.Key, iter.Item().Key)

	// Test SeekForPrev
	iter.SeekForPrev(entry2.Key)
	assert.True(t, iter.Valid())
	assert.Equal(t, entry2.Key, iter.Item().Key)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Logf("iter key %s, value %s", iter.Item().Key, string(iter.Item().ValueStruct.Value))
	}
}
