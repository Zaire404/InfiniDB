package util

import (
	"hash/crc32"

	. "github.com/rryqszq4/go-murmurhash"
)

var (
	CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)
)

func Hash(data []byte) uint32 {
	var seed uint32 = 0xdeadbeef
	return MurmurHash3_x86_32(data, seed)
}

func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, CastagnoliTable)
}
