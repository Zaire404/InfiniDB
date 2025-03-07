package infinidb

type Options struct {
	WorkDir            string
	MemTableSize       uint32
	SSTableSize        uint32
	ValueThreshold     int64
	ValueLogFileSize   int
	ValueLogMaxEntries uint32
}
