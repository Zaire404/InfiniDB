package util


import (
	"encoding/binary"
	"errors"
	"strconv"
	"unsafe"
)

const (
	VptrSize        = unsafe.Sizeof(ValuePtr{})
	BitValuePointer = 1 << 0
)

type ValuePtr struct {
	Len    uint32 // entryCodec length
	Offset uint32 // offset in value log file
	FID    uint32
}

func (vp ValuePtr) String() string {
	return "ValuePtr{Len: " + strconv.Itoa(int(vp.Len)) + ", Offset: " + strconv.Itoa(int(vp.Offset)) + ", FID: " + strconv.Itoa(int(vp.FID)) + "}"
}

func (vp ValuePtr) Encode() []byte {
	out := make([]byte, VptrSize)
	*(*ValuePtr)(unsafe.Pointer(&out[0])) = vp
	return out
}

func (vp *ValuePtr) Decode(in []byte) {
	if len(in) < VptrSize {
		panic("input slice too short for ValuePtr decoding")
	}
	copy(((*[VptrSize]byte)(unsafe.Pointer(vp))[:]), in[:VptrSize])
}

func IsValuePtr(entry *Entry) bool {
	return entry.ValueStruct.Meta&BitValuePointer > 0
}

type VLogHeader struct {
	KeyLen   uint32
	ValueLen uint32
}

func (h *VLogHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	return index
}

func (h *VLogHeader) Decode(in []byte) (int, error) {
	klen, klenN := binary.Uvarint(in)
	if klenN <= 0 {
		return 0, errors.New("value log header: invalid key length")
	}
	h.KeyLen = uint32(klen)

	vlen, vlenN := binary.Uvarint(in[klenN:])
	if vlenN <= 0 {
		return 0, errors.New("value log header: invalid value length")
	}
	h.ValueLen = uint32(vlen)

	return klenN + vlenN, nil
}

func (h *VLogHeader) DecodeFromHashReader(reader *HashReader) (int, error) {
	var err error

	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	return reader.BytesRead, nil
}
