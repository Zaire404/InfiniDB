package file

import (
	"fmt"
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
