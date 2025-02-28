package lsm

import (
	"github.com/Zaire404/InfiniDB/skl"
	"github.com/Zaire404/InfiniDB/util"
)

type MemTable struct {
	sl    *skl.SkipList
	ref   uint32
	arena *util.Arena
}

func newMemTable() *MemTable {
	return &MemTable{
		sl:    skl.NewSkipList(1 << 20),
		arena: util.NewArena(1 << 20),
	}
}

func (m *MemTable) Size() uint32 {
	return m.sl.MemSize()
}

func (m *MemTable) set(entry *util.Entry) {
	m.sl.Add(entry)
}

func (m *MemTable) get(key []byte) (*util.Entry, error) {
	vs, err := m.sl.Search(key)
	return &util.Entry{Key: key, ValueStruct: vs}, err
}
