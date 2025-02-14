package lsm

import "log"

type Options struct {
	WorkDir                      string
	MemTableSize                 uint32
	SSTableSize                  uint32
	BlockSize                    uint32 // Max is 100MB
	BloomFilterFalsePositiveRate float64
	LevelCount                   uint16
	Logger                       *log.Logger
}
