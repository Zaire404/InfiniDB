package util

import "encoding/binary"

func BytesToUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func BytesToUint32Slice(b []byte) []uint32 {
	u := make([]uint32, len(b)/4)
	for i := 0; i < len(b); i += 4 {
		u[i/4] = binary.LittleEndian.Uint32(b[i:])
	}
	return u
}

func BytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func Uint32SliceToBytes(u []uint32) []byte {
	b := make([]byte, len(u)*4)
	for i, v := range u {
		binary.LittleEndian.PutUint32(b[i*4:], v)
	}
	return b
}

func Uint32ToBytes(u uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, u)
	return b
}
