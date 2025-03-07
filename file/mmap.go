package file

import (
	"bytes"
	"fmt"
	"io"
	"os"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/util"

	"github.com/pkg/errors"
)

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func OpenMmapFile(filePath string, flag int, size int) (*MmapFile, error) {
	if size < 0 {
		return nil, errors.Errorf("invalid size: %d", size)
	}

	fd, err := os.OpenFile(filePath, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s", filePath)
	}

	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to stat file %s", filePath)
	}
	fileSize := fileInfo.Size()
	if fileSize == 0 {
		// file is empty, truncate it
		if err := fd.Truncate(int64(size)); err != nil {
			return nil, errors.Wrapf(err, "failed to truncate file %s", filePath)
		}
		fileSize = int64(size)
		// To be optimized
		go fd.Sync()
	}
	writable := flag&os.O_RDWR == os.O_RDWR
	buf, err := util.Mmap(fd, int(fileSize), writable)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to mmap file %s with size: %d", filePath, fileSize)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return util.Msync(m.Data)
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := util.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, ErrReadOutOfBound
	}
	return m.Data[off : off+sz], nil
}

func (m *MmapFile) Delete() {
	os.Rename(m.Fd.Name(), m.Fd.Name()+".del")
}

func (m *MmapFile) Truncate(size int64) (err error) {
	if err = m.Sync(); err != nil {
		return errors.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err = m.Fd.Truncate(size); err != nil {
		return errors.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	m.Data, err = util.Mremap(m.Data, int(size))
	return err
}

// AppendBuffer appends a buffer to the mmap file at the specified offset.
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	const oneGB = 1 << 30

	dataSize := len(m.Data)
	bufferSize := len(buf)
	endOffset := int(offset) + bufferSize

	if endOffset > dataSize {
		growBy := dataSize
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < bufferSize {
			growBy = bufferSize
		}
		if err := m.Truncate(int64(offset) + int64(growBy)); err != nil {
			return err
		}
	}

	copiedLen := copy(m.Data[offset:endOffset], buf)
	if copiedLen != bufferSize {
		return errors.Errorf("copiedLen != bufferSize: AppendBuffer failed")
	}
	return nil
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	// return &mmapReader{
	// 	Data:   m.Data,
	// 	offset: offset,
	// }
	r := bytes.NewReader(m.Data)
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		panic(err)
	}
	return r
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}
