package util

import (
	. "github.com/rryqszq4/go-murmurhash"
)

func Hash(data []byte) uint32 {
	var seed uint32 = 0xdeadbeef
	return MurmurHash3_x86_32(data, seed)
}
