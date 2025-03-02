package util

import "encoding/binary"

type ValueStruct struct {
	Value    []byte
	ExpireAt uint64
}

func (v *ValueStruct) EncodedSize() uint32 {
	return uint32(len(v.Value)) + sizeVarint(v.ExpireAt)
}

func (v *ValueStruct) DecodeValue(b []byte) {
	n, sz := binary.Uvarint(b)
	v.ExpireAt = n
	v.Value = b[sz:]
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
func (v *ValueStruct) EncodeValue(b []byte) uint32 {
	sz := binary.PutUvarint(b, v.ExpireAt)
	return uint32(sz + copy(b[sz:], v.Value))
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
