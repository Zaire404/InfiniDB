package infinidb

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

const (
	VLogFileExt            = ".vlog"
	MaxVLogFileSize uint32 = math.MaxUint32
)

type valueLog struct {
	db              *DB
	maxFID          uint32
	opt             *Options
	filesMap        map[uint32]*file.VLogFile
	fidsToBeDeleted map[uint32]struct{}
	writeLogOffset  uint32
	numEntries      uint32
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

type request struct {
	entries []*util.Entry
	Ptrs    []*util.ValuePtr
	Wg      sync.WaitGroup
	ref     int32
}

func (req *request) reset() {
	req.entries = req.entries[:0]
	req.Ptrs = req.Ptrs[:0]
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	if atomic.AddInt32(&req.ref, -1) == 0 {
		req.entries = nil
		requestPool.Put(req)
	}
}

func (req *request) estimateWriteSize() int {
	size := 0
	for _, e := range req.entries {
		// 10 bytes for key length and value length
		size += 10 + len(e.Key) + len(e.ValueStruct.Value) + crc32.Size
	}
	return size
}

func (vlog *valueLog) getWriteLogOffset() uint32 {
	return atomic.LoadUint32(&vlog.writeLogOffset)
}

// loadFilesMap initializes the filesMap by reading the files in the work directory.
// It also sets the maxFID to the highest FID found and load the vlog file to memory.
func (vlog *valueLog) loadFilesMap() error {
	vlog.filesMap = make(map[uint32]*file.VLogFile)
	files, err := os.ReadDir(vlog.opt.WorkDir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), VLogFileExt) {
			continue
		}
		fid, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), VLogFileExt), 10, 32)
		if err != nil {
			return err
		}
		vlogFile, err := file.NewVLogFile(&file.Options{
			Dir:     vlog.opt.WorkDir,
			FID:     fid,
			MaxSize: vlog.opt.ValueLogFileSize,
		})
		if err != nil {
			return errors.Wrap(err, "failed to init vlog file")
		}
		vlog.filesMap[vlogFile.FID] = vlogFile
		if vlogFile.FID > vlog.maxFID {
			vlog.maxFID = vlogFile.FID
		}
	}
	return nil
}

// createVLogFile creates a new vlog file with the given FID and initializes the file.
// It also updates the maxFID and numEntries.
func (vlog *valueLog) createVLogFile(fid uint32) (*file.VLogFile, error) {
	opt := &file.Options{
		Dir:     vlog.opt.WorkDir,
		FID:     uint64(fid),
		MaxSize: 2 * vlog.opt.ValueLogFileSize,
	}

	vlogFile, err := file.NewVLogFile(opt)
	if err != nil {
		panic(err)
	}
	if err = vlogFile.Open(opt); err != nil {
		panic(err)
	}

	if err := util.SyncDir(vlog.opt.WorkDir); err != nil {
		os.Rename(vlogFile.FD().Name(), vlogFile.FD().Name()+".del")
		return nil, errors.Errorf("failed to sync dir: %v", err)
	}
	atomic.StoreUint32(&vlog.maxFID, fid)
	vlog.filesMap[fid] = vlogFile
	atomic.StoreUint32(&vlog.writeLogOffset, 0)
	// TODO: need to atomic store?
	vlog.numEntries = 0
	return vlogFile, nil
}

func (vlog *valueLog) load(replayFn util.LogEntry) error {
	if err := vlog.loadFilesMap(); err != nil {
		return errors.Wrap(err, "failed to init files map")
	}
	if len(vlog.filesMap) == 0 {
		_, err := vlog.createVLogFile(0)
		return errors.Wrap(err, "failed to create new vlog file")
	}
	// vlogs := vlog.sortedFIDs()
	for _, vlogFile := range vlog.filesMap {
		if err := vlogFile.Open(&file.Options{
			Dir:     vlog.opt.WorkDir,
			MaxSize: vlog.opt.ValueLogFileSize,
			FID:     uint64(vlogFile.FID),
		}); err != nil {
			return errors.Wrap(err, "failed to open vlog file")
		}
		// if err := vlog.replayLog(vlogFile, 0, replayFn); err != nil {
		// 	return errors.Wrap(err, "failed to replay log")
		// }
	}
	return nil
}

func (vlog *valueLog) newValuePtr(e *util.Entry) (*util.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()
	req.entries = []*util.Entry{e}
	req.Wg.Add(1)
	req.IncrRef()
	defer req.DecrRef()
	err := vlog.writeReqs([]*request{req})
	return req.Ptrs[0], err
}

func (vlog *valueLog) sortedFIDs() []uint32 {
	fids := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if _, ok := vlog.fidsToBeDeleted[fid]; ok {
			continue
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	return fids
}

// validateReqs validates the given requests to ensure that the value log file size is not exceeded.
func (vlog *valueLog) validateReqs(reqs []*request) error {
	curOffset := uint64(vlog.getWriteLogOffset())
	for _, req := range reqs {
		size := req.estimateWriteSize()
		estimateOffset := curOffset + uint64(size)
		if estimateOffset > uint64(MaxVLogFileSize) {
			return errors.Errorf("value log file size exceeded: %d", estimateOffset)
		}
		if int(estimateOffset) >= vlog.opt.ValueLogFileSize {
			curOffset = 0
			continue
		}
		curOffset = estimateOffset
	}
	return nil
}

func (vlog *valueLog) writeReqs(reqs []*request) error {
	if err := vlog.validateReqs(reqs); err != nil {
		return err
	}

	maxFID := atomic.LoadUint32(&vlog.maxFID)
	curVlogFile := vlog.filesMap[maxFID]
	var buf bytes.Buffer
	flushWrites := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes()
		offset := vlog.getWriteLogOffset()
		if err := curVlogFile.Write(offset, data); err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", curVlogFile.FileName())
		}
		buf.Reset()
		atomic.AddUint32(&vlog.writeLogOffset, uint32(len(data)))
		curVlogFile.StoreSize(vlog.writeLogOffset)
		return nil
	}
	commitToDisk := func() error {
		if err := flushWrites(); err != nil {
			return err
		}
		if vlog.getWriteLogOffset() > uint32(vlog.opt.ValueLogFileSize) ||
			vlog.numEntries > vlog.opt.ValueLogMaxEntries {
			if err := curVlogFile.DoneWriting(vlog.getWriteLogOffset()); err != nil {
				return err
			}

			nextFID := atomic.AddUint32(&vlog.maxFID, 1)
			newVLogFile, err := vlog.createVLogFile(nextFID)
			if err != nil {
				return err
			}
			curVlogFile = newVLogFile
		}
		return nil
	}
	for i := range reqs {
		req := reqs[i]
		req.Ptrs = req.Ptrs[:0]
		var entriesWritten int
		for j := range req.entries {
			e := req.entries[j]
			// TODO
			if vlog.db.shouldWriteValueToLSM(e) {
				req.Ptrs = append(req.Ptrs, &util.ValuePtr{})
				continue
			}
			var vp util.ValuePtr

			vp.FID = curVlogFile.FID
			vp.Offset = vlog.getWriteLogOffset() + uint32(buf.Len())
			vpLen, err := curVlogFile.EncodeEntry(e, &buf, vp.Offset)
			if err != nil {
				return err
			}
			vp.Len = uint32(vpLen)
			req.Ptrs = append(req.Ptrs, &vp)
			entriesWritten++

			if buf.Len() > vlog.db.opt.ValueLogFileSize {
				if err := flushWrites(); err != nil {
					return err
				}
			}
		}
		vlog.numEntries += uint32(entriesWritten)
		shouldWriteNow :=
			vlog.getWriteLogOffset()+uint32(buf.Len()) > uint32(vlog.opt.ValueLogFileSize) ||
				vlog.numEntries > uint32(vlog.opt.ValueLogMaxEntries)
		if shouldWriteNow {
			if err := commitToDisk(); err != nil {
				return err
			}
		}
	}
	return commitToDisk()
}

func (vlog *valueLog) replayLog(vlogFile *file.VLogFile, offset uint32, replayFn util.LogEntry) error {
	endOffset, err := vlog.iterate(vlogFile, offset, replayFn)
	if err != nil {
		return errors.Wrap(err, "failed to iterate vlog file")
	}
	if endOffset == vlogFile.Size() {
		return nil
	}

	if err = vlogFile.Truncate(int64(endOffset)); err != nil {
		return errors.Wrap(err, "failed to truncate vlog file")
	}
	return nil
}

// iterate iterates over the value log file starting from the given offset.
// It reads the log entries and calls the given function for each entry.
// It returns the end offset of the iteration.
func (vlog *valueLog) iterate(vlogFile *file.VLogFile, offset uint32, fn util.LogEntry) (uint32, error) {
	validEndOffset := offset
	if offset == vlogFile.Size() {
		return offset, nil
	}
	if _, err := vlogFile.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(vlogFile.FD())
	r := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: 0,
	}

loop:
	for {
		e, err := r.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		}

		var vp util.ValuePtr
		vp.Len = uint32(e.HeaderLen) + uint32(len(e.Key)) + uint32(len(e.ValueStruct.Value)) + crc32.Size
		r.RecordOffset += vp.Len
		vp.Offset = e.Offset
		vp.FID = vlogFile.FID
		validEndOffset = r.RecordOffset
		if err := fn(e, &vp); err != nil {
			return 0, errors.Errorf("iterate failed: %v", err)
		}
	}
	return validEndOffset, nil
}

func (vlog *valueLog) readValuePtr(vp *util.ValuePtr) (*util.Entry, error) {
	// if vp.FID == vlog.maxFID {
	// 	if vp.Offset >= vlog.getWriteLogOffset() {
	// 		return nil, errors.Errorf("invalid offset: %d", vp.Offset)
	// 	}
	// }
	vlogFile, ok := vlog.filesMap[vp.FID]
	if !ok {
		return nil, errors.Errorf("vlog file not found for FID: %d", vp.FID)
	}

	entryCodec, err := vlogFile.ReadValuePtr(vp)
	if err != nil {
		return nil, err
	}
	return vlogFile.DecodeEntry(entryCodec, vp.Offset)
}

type SafeRead struct {
	K            []byte
	V            []byte
	RecordOffset uint32
}

func (r *SafeRead) MakeEntry(reader io.Reader) (*util.Entry, error) {
	hashReader := util.NewHashReader(reader)
	var header util.VLogHeader
	headerLen, err := header.DecodeFromHashReader(hashReader)
	if err != nil {
		return nil, err
	}
	// Key length must not exceed the maximum value of a uint16 (65535)
	if header.KeyLen > uint32(1<<16) {
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
	return e, nil
}
