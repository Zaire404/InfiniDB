package file

import (
	"os"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

type SSTable struct {
	file           *MmapFile
	maxKey         []byte
	minKey         []byte
	indexTable     *proto.IndexTable
	fid            uint64
	indexLen       uint32
	idxStart       uint32
	hasBloomFilter bool
}

func OpenSSTable(opt *Options) (*SSTable, error) {
	mmapFile, err := OpenMmapFile(opt.FilePath, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	if err != nil {
		return nil, err
	}
	sst := &SSTable{
		file: mmapFile,
	}
	return sst, nil
}

func (sst SSTable) HasBloomFilter() bool {
	return sst.hasBloomFilter
}

func (sst *SSTable) Init() error {
	err := sst.initIndexTable()
	if err != nil {
		return err
	}

	blockOffsets := sst.indexTable.GetOffsets()
	// minKey = first baseKey
	baseKey := blockOffsets[0].GetKey()
	sst.minKey = make([]byte, len(baseKey))
	copy(sst.minKey, baseKey)

	// maxKey is set in openTable

	return nil
}

func (sst *SSTable) initIndexTable() error {
	// indexChecksumLen
	offset := len(sst.file.Data)
	offset -= 4
	buf, err := sst.readMmap(offset, 4)
	if err != nil {
		panic(err)
	}
	indexChecksumLen := int(util.BytesToUint32(buf))
	if indexChecksumLen < 0 {
		return errors.New("checksumLen < 0")
	}

	// indexChecksum
	offset -= indexChecksumLen
	indexChecksum, err := sst.readMmap(offset, indexChecksumLen)
	if err != nil {
		panic(err)
	}

	// indexDataLen
	offset -= 4
	buf, err = sst.readMmap(offset, 4)
	if err != nil {
		panic(err)
	}
	sst.indexLen = util.BytesToUint32(buf)

	// indexData
	offset -= int(sst.indexLen)
	sst.idxStart = uint32(offset)
	indexData, err := sst.readMmap(offset, int(sst.indexLen))
	if err != nil {
		panic(err)
	}
	if !util.VerifyCheckSum(indexData, indexChecksum) {
		return ErrChecksum
	}
	sst.indexTable = &proto.IndexTable{}
	err = proto.Unmarshal(indexData, sst.indexTable)
	if err != nil {
		return err
	}
	sst.hasBloomFilter = len(sst.indexTable.BloomFilter) > 0
	if len(sst.indexTable.GetOffsets()) <= 0 {
		return errors.New("idxTable.GetOffsets() is empty")
	}
	return nil
}

func (sst *SSTable) readMmap(offset int, sz int) ([]byte, error) {
	if len(sst.file.Data) < offset+sz || len(sst.file.Data) <= 0 {
		return nil, errors.Errorf("SSTable readMmap %s", ErrReadOutOfBound)
	} else {
		return sst.file.Data[offset : offset+sz], nil
	}
}

func (sst *SSTable) Bytes(off int, sz int) ([]byte, error) {
	return sst.file.Bytes(off, sz)
}

func (sst *SSTable) MinKey() []byte {
	return sst.minKey
}

func (sst *SSTable) MaxKey() []byte {
	return sst.maxKey
}

func (sst *SSTable) SetMaxKey(key []byte) {
	sst.maxKey = key
}

func (sst *SSTable) IndexTable() *proto.IndexTable {
	return sst.indexTable
}

func (sst *SSTable) Size() int64 {
	stat, err := sst.file.Fd.Stat()
	if err != nil {
		panic(err)
	}
	return stat.Size()
}

func (sst *SSTable) Delete() {
	sst.file.Delete()
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}
