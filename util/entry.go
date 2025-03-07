package util

import "encoding/binary"

type ValueStruct struct {
	Value    []byte
	ExpireAt uint64
	Meta     byte
}

func (v *ValueStruct) EncodedSize() uint32 {
	// 1 byte for meta
	return uint32(len(v.Value)) + sizeVarint(v.ExpireAt) + 1
}

func (v *ValueStruct) DecodeValue(b []byte) {
	n, sz := binary.Uvarint(b)
	v.ExpireAt = n
	v.Value = b[sz : len(b)-1]
	v.Meta = b[len(b)-1]
}

func sizeVarint(v uint64) uint32 {
	size := uint32(1)
	for v >= 128 {
		v >>= 7
		size++
	}
	return size
}

// EncodeValue encodes the value struct into the byte slice.
func (v *ValueStruct) EncodeValue(b []byte) (encSize uint32) {
	sz := binary.PutUvarint(b, v.ExpireAt)
	encSize = uint32(sz + copy(b[sz:], v.Value))
	encSize += uint32(copy(b[encSize:], []byte{v.Meta}))
	return encSize
}

type Entry struct {
	Key         []byte
	ValueStruct ValueStruct
	Offset      uint32
	HeaderLen   int
}

func NewEntry(key []byte, value []byte) *Entry {
	return &Entry{
		Key: key,
		ValueStruct: ValueStruct{
			Value: value,
		},
	}
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) LogHeaderLen() int {
	return e.HeaderLen
}

func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}
