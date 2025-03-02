package file

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"sync"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/log"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

type WalFile struct {
	lock    *sync.RWMutex
	file    *MmapFile
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

func (wf *WalFile) FID() uint64 {
	return wf.opts.FID
}

func (wf *WalFile) Close() error {
	fileName := wf.file.Fd.Name()
	if err := wf.file.Close(); err != nil {
		return err
	}
	return os.Rename(fileName, fileName+".del")
}

func (wf *WalFile) Name() string {
	return wf.file.Fd.Name()
}

// Size
func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FilePath, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	wf := &WalFile{
		lock: &sync.RWMutex{},
		file: mf,
		opts: opt,
		buf:  &bytes.Buffer{},
		size: uint32(len(mf.Data)),
	}
	if err != nil {
		log.Logger.Errorf("open wal file: %s, size: %d", wf.Name(), wf.size)
	}
	return wf
}

func (wf *WalFile) Write(entry *util.Entry) error {
	// Lock the WAL file for writing
	wf.lock.Lock()
	defer wf.lock.Unlock()

	// Encode the entry into the buffer and get the encoded length
	encodedLen := util.WalCodec(wf.buf, entry)
	buf := wf.buf.Bytes()

	// Append the encoded data to the file
	if err := wf.file.AppendBuffer(wf.writeAt, buf[:encodedLen]); err != nil {
		return errors.Errorf("failed to append buffer: %s", err)
	}

	// Update the write position
	wf.writeAt += uint32(encodedLen)
	return nil
}

// Iterate reads the WAL file from the given offset and calls the provided function for each log entry.
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn util.LogEntry) (uint32, error) {
	// For now, read directly from file, because it allows
	reader := bufio.NewReader(wf.file.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		WF:           wf,
	}
	var validEndOffset uint32 = offset
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e.IsZero():
			break loop
		}

		var vp util.ValuePtr
		size := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.ValueStruct.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset

		// TODO: vlog
		if err := fn(e, &vp); err != nil {
			if err == ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fi, err := wf.file.Fd.Stat(); err != nil {
		return errors.Errorf("while file.stat on file: %s, error: %v\n", wf.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.file.Truncate(end)
}

// SafeRead encapsulates a safely read record from a Write-Ahead Log (WAL) file.
// It holds the key (K) and value (V) as byte slices, providing the actual data of the record.
// The RecordOffset field indicates the record's position within the WAL file,
// while the WF field stores a reference to the associated WalFile instance.
type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	WF           *WalFile
}

// MakeEntry reads a record from the given reader and returns an Entry instance.
// It returns an error if the record is truncated, the checksum is invalid, or an I/O error occurs.
func (r *SafeRead) MakeEntry(reader io.Reader) (*util.Entry, error) {
	hashReader := util.NewHashReader(reader)
	var header util.WalHeader
	headerLen, err := header.Decode(hashReader)
	if err != nil {
		return nil, err
	}
	if header.KeyLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, ErrTruncate
	}
	kl := int(header.KeyLen)
	if kl == 0 {
		return nil, io.EOF
	}
	if cap(r.K) < kl {
		r.K = make([]byte, 2*kl)
	}
	vl := int(header.ValueLen)
	if cap(r.V) < vl {
		r.V = make([]byte, 2*vl)
	}

	e := &util.Entry{
		Offset:    r.RecordOffset,
		HeaderLen: headerLen,
	}
	buf := make([]byte, header.KeyLen+header.ValueLen)
	if _, err := io.ReadFull(hashReader, buf[:]); err != nil {
		if err == io.EOF {
			err = ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:header.KeyLen]
	e.ValueStruct.Value = buf[header.KeyLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = ErrTruncate
		}
		return nil, err
	}
	crc := util.BytesToUint32(crcBuf[:])
	if crc != hashReader.Sum32() {
		return nil, ErrChecksum
	}
	e.ValueStruct.ExpireAt = header.ExpireAt
	return e, nil
}
