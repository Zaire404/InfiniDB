package util

import (
	"hash"
	"hash/crc32"
	"io"

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

type HashReader struct {
	Reader    io.Reader
	Hash      hash.Hash32
	BytesRead int // Number of bytes read.
}

// NewHashReader creates a new HashReader with the given io.Reader.
func NewHashReader(r io.Reader) *HashReader {
	return &HashReader{
		Reader: r,
		Hash:   crc32.New(CastagnoliTable),
	}
}

// Read reads len(p) bytes from the reader and updates the hash. Returns the number of bytes read and any error encountered.
func (hr *HashReader) Read(p []byte) (int, error) {
	n, err := hr.Reader.Read(p)
	if n > 0 {
		hr.BytesRead += n
		if _, hashErr := hr.Hash.Write(p[:n]); hashErr != nil {
			return n, hashErr
		}
	}
	return n, err
}

// ReadByte reads exactly one byte from the reader and updates the hash. Returns the byte read and any error encountered.
func (hr *HashReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := hr.Read(b[:])
	return b[0], err
}

// Sum32 returns the current CRC32 checksum of the data read so far.
func (hr *HashReader) Sum32() uint32 {
	return hr.Hash.Sum32()
}
