package infinidb

type Options struct {
	WorkDir                      string
	MemTableSize                 uint32
	SSTableSize                  uint32
	ValueThreshold               int64
	ValueLogFileSize             int
	ValueLogMaxEntries           uint32
	BaseTableSize                int64
	BaseLevelSize                int64
	BlockSize                    uint32
	BloomFilterFalsePositiveRate float64
	NumLevelZeroTables           int
	CompactThreadCount           int
	LevelCount                   int
	LevelSizeMultiplier          int
}

func DefaultOptions(path string) Options {
	return Options{
		WorkDir:             path,
		MemTableSize:        16 << 20,
		SSTableSize:         16 << 20,
		ValueLogFileSize:    16 << 20,
		ValueLogMaxEntries:  100000,
		ValueThreshold:      100,
		BaseTableSize:       8 << 20,
		BaseLevelSize:       256 << 20,
		LevelSizeMultiplier: 10,
		LevelCount:          7,

		CompactThreadCount:           2, // Run at least 2 compactors. Zero-th compactor prioritizes L0.
		NumLevelZeroTables:           5,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    4 * 1024,
	}
}
