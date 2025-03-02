package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"path/filepath"
)

const WalFileExt string = ".wal"

type LogEntry func(e *Entry, vp *ValuePtr) error

func WalFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d%s", fid, WalFileExt))
}

type WalHeader struct {
	KeyLen   uint32
	ValueLen uint32
	Meta     byte
	ExpireAt uint64
}

const maxHeaderSize = binary.MaxVarintLen64 * 4

// Encode encodes the header to the buffer. Returns the number of bytes written.
func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpireAt)
	return index
}

// Decode decodes the header from the reader. Returns the number of bytes read.
func (h *WalHeader) Decode(reader *HashReader) (int, error) {
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

	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpireAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// WalCodec encodes the entry to the buffer. Returns the number of bytes written.
// The format is:
//   - header: key length, value length, meta, expires at.
//   - key.
//   - value.
//   - crc32 hash.
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:   uint32(len(e.Key)),
		ValueLen: uint32(len(e.ValueStruct.Value)),
		ExpireAt: e.ValueStruct.ExpireAt,
	}

	hash := crc32.New(CastagnoliTable)
	// write to buffer and hash
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	if _, err := writer.Write(headerEnc[:sz]); err != nil {
		panic(err)
	}
	if _, err := writer.Write(e.Key); err != nil {
		panic(err)
	}
	if _, err := writer.Write(e.ValueStruct.Value); err != nil {
		panic(err)
	}
	// write crc32 hash.
	if _, err := buf.Write(Uint32ToBytes(hash.Sum32())); err != nil {
		panic(err)
	}
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.ValueStruct.Value) + crc32.Size
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
