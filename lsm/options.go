package lsm

import "log"

type Options struct {
	WorkDir                      string
	MemTableSize                 uint32
	SSTableSize                  uint32
	BlockSize                    uint32 // Max is 100MB
	BloomFilterFalsePositiveRate float64
	LevelCount                   uint16
	CompactThreadCount           uint16
	BaseLevelSize                int64
	BaseTableSize                int64
	LevelSizeMultiplier          int
	NumLevelZeroTables           int
	Logger                       *log.Logger
}
