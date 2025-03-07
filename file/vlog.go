package file

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

const MaxHeaderSize = 10

type VLogFile struct {
	Lock sync.RWMutex
	FID  uint32
	size uint32
	file *MmapFile
}

func NewVLogFile(opt *Options) (*VLogFile, error) {
	return &VLogFile{
		Lock: sync.RWMutex{},
		FID:  uint32(opt.FID),
	}, nil
}

func (f *VLogFile) Reload() error {
	fileStat, err := f.file.Fd.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to stat vlog file")
	}
	size := fileStat.Size()
	f.size = uint32(size)
	return nil
}

func (f *VLogFile) Open(opt *Options) error {
	file, err := OpenMmapFile(util.VLogFilePath(opt.Dir, uint32(opt.FID)), os.O_CREATE|os.O_RDWR, opt.MaxSize)
	if err != nil {
		return err
	}
	fileStat, err := file.Fd.Stat()
	if err != nil {
		return err
	}
	f.size = uint32(fileStat.Size())
	f.file = file
	return nil
}

func (f *VLogFile) Size() uint32 {
	return atomic.LoadUint32(&f.size)
}

func (f *VLogFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Fd.Seek(offset, whence)
}

func (f *VLogFile) FD() *os.File {
	return f.file.Fd
}

func (f *VLogFile) FileName() string {
	return f.file.Fd.Name()
}

func (f *VLogFile) Write(offset uint32, buf []byte) (err error) {
	return f.file.AppendBuffer(offset, buf)
}

func (f *VLogFile) StoreSize(size uint32) {
	atomic.StoreUint32(&f.size, size)
}

func (f *VLogFile) Truncate(size int64) error {
	return f.file.Truncate(size)
}

func (f *VLogFile) withRLock(fn func() ([]byte, error)) ([]byte, error) {
	f.Lock.RLock()
	defer f.Lock.RUnlock()
	return fn()
}

func (f *VLogFile) ReadValuePtr(vp *util.ValuePtr) (buf []byte, err error) {
	return f.withRLock(func() ([]byte, error) {
		offset := vp.Offset
		dataSize := int64(len(f.file.Data))
		entryCodecLen := vp.Len
		vlogFileSize := atomic.LoadUint32(&f.size)
		if int64(offset) >= dataSize || int64(offset+entryCodecLen) > dataSize ||
			// Ensure that the read is within the file's actual size. It might be possible that
			// the offset+valsz length is beyond the file's actual size. This could happen when
			// dropAll and iterations are running simultaneously.
			int64(offset+entryCodecLen) > int64(vlogFileSize) {
			err = io.EOF
		} else {
			buf, err = f.file.Bytes(int(offset), int(entryCodecLen))
		}
		return buf, err
	})
}

func (f *VLogFile) DoneWriting(offset uint32) error {
	if err := f.file.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync vlog file")
	}
	f.Lock.Lock()
	defer f.Lock.Unlock()

	if err := f.file.Truncate(int64(offset)); err != nil {
		return errors.Wrap(err, "failed to truncate vlog file")
	}

	if err := f.Reload(); err != nil {
		return errors.Wrap(err, "failed to reload vlog file")
	}
	return nil
}

func (f *VLogFile) EncodeEntry(e *util.Entry, buf *bytes.Buffer, offset uint32) (int, error) {
	h := util.VLogHeader{
		KeyLen:   uint32(len(e.Key)),
		ValueLen: uint32(len(e.ValueStruct.Value)),
	}

	hash := crc32.New(util.CastagnoliTable)
	writer := io.MultiWriter(buf, hash)

	var headerEnc [MaxHeaderSize]byte
	size := h.Encode(headerEnc[:])
	if _, err := writer.Write(headerEnc[:size]); err != nil {
		panic(err)
	}

	if _, err := writer.Write(e.Key); err != nil {
		panic(err)
	}
	if _, err := writer.Write(e.ValueStruct.Value); err != nil {
		panic(err)
	}

	if _, err := buf.Write(util.Uint32ToBytes(hash.Sum32())); err != nil {
		panic(err)
	}

	return len(headerEnc[:size]) + len(e.Key) + len(e.ValueStruct.Value) + crc32.Size, nil
}

func (f *VLogFile) DecodeEntry(buf []byte, offset uint32) (*util.Entry, error) {
	var h util.VLogHeader
	headerLen, err := h.Decode(buf)
	if err != nil {
		return nil, err
	}
	kv := buf[headerLen:]
	e := &util.Entry{
		Offset: offset,
		Key:    kv[:h.KeyLen],
		ValueStruct: util.ValueStruct{
			Value:    kv[h.KeyLen : h.KeyLen+h.ValueLen],
			ExpireAt: 0,
		},
	}
	return e, nil
}
