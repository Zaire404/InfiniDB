package file

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/log"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/Zaire404/InfiniDB/util"
)

type ManifestFile struct {
	file     *os.File
	lock     sync.Mutex
	opt      *Options
	manifest *Manifest
}

type Manifest struct {
	Levels      []*LevelManifest
	Tables      map[uint64]*TableManifest
	CreateCount int
	DeleteCount int
}

type TableManifest struct {
	Level    uint8
	Checksum []byte
}

type LevelManifest struct {
	Tables map[uint64]struct{}
}

func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, ManifestName)
	mf := &ManifestFile{
		opt:      opt,
		manifest: newManifest(),
	}

	var err error
	mf.file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			// file exists
			return mf, err
		}
		// file not exists
		log.Logger.Infof("manifest file not exists, create a new one")
		err := mf.RewriteManifestFile(opt.Dir)
		if err != nil {
			log.Logger.Errorf("rewrite manifest file failed: %v", err)
			return mf, err
		}
		return mf, nil
	}
	log.Logger.Infof("manifest exists, load it")
	var truncOffset int64
	if truncOffset, err = mf.manifest.ReplayManifestFile(mf.file); err != nil {
		log.Logger.Errorf("replay manifest file failed: %v", err)
		mf.file.Close()
		return mf, err
	}
	if err := mf.file.Truncate(truncOffset); err != nil {
		log.Logger.Errorf("truncate manifest file failed: %v", err)
		mf.file.Close()
		return mf, err
	}
	if _, err := mf.file.Seek(0, io.SeekEnd); err != nil {
		log.Logger.Errorf("seek manifest file failed: %v", err)
		mf.file.Close()
		return mf, err
	}
	return mf, nil
}

// RewriteManifestFile rewrites the manifest file with the current manifest and update the file handle
func (mf *ManifestFile) RewriteManifestFile(dir string) error {
	var err error
	mf.manifest.flush(filepath.Join(dir, ReManifestName))
	if mf.file != nil {
		if err := mf.file.Close(); err != nil {
			log.Logger.Errorf("close manifest file failed: %v", err)
			return err
		}
	}
	// rename is atomic
	if err := os.Rename(filepath.Join(dir, ReManifestName), filepath.Join(dir, ManifestName)); err != nil {
		log.Logger.Errorf("rename manifest file failed: %v", err)
		return err
	}
	if mf.file, err = os.OpenFile(filepath.Join(dir, ManifestName), os.O_RDWR, 0); err != nil {
		log.Logger.Errorf("open manifest file failed: %v", err)
		return err
	}
	return nil
}

func (mf *ManifestFile) AddTable(id uint64, level int) error {
	return mf.addChanges([]*proto.ManifestChange{newManifestChange(id, level, nil)})
}

func (mf *ManifestFile) addChanges(changes []*proto.ManifestChange) error {
	commit := proto.ManifestCommit{Changes: changes}
	data, err := proto.Marshal(&commit)
	if err != nil {
		return err
	}

	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := mf.manifest.applyCommit(&commit); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/RewriteRatio and it's big enough to care
	if mf.manifest.DeleteCount > RewriteThreshold &&
		mf.manifest.DeleteCount > RewriteRatio*(mf.manifest.CreateCount-mf.manifest.DeleteCount) {
		if err := mf.RewriteManifestFile(mf.opt.Dir); err != nil {
			return err
		}
	} else {
		buf := make([]byte, 8+len(data))
		len := util.Uint32ToBytes(uint32(len(buf)))
		checksum := util.Uint32ToBytes(util.Checksum(buf))
		copy(buf, len)
		copy(buf[4:], checksum)
		copy(buf[8:], data)
		if _, err := mf.file.Write(buf); err != nil {
			return err
		}
	}
	if err = mf.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}

func newManifest() *Manifest {
	return &Manifest{
		Levels: make([]*LevelManifest, 0),
		Tables: make(map[uint64]*TableManifest),
	}
}

func (mf *ManifestFile) SyncManifestWithDir(dir string) error {
	idMap, err := util.CollectIDMap(dir)
	if err != nil {
		return err
	}
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			log.Logger.Errorf("table %d not exists in dir", id)
			return ErrTableNotExists
		}
	}

	// if table exists in dir but not in manifest, remove it
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			log.Logger.Infof("table %d not exists in manifest, remove it", id)
			sstPath := mf.opt.Dir + "/" + util.GenSSTName(id)
			if err := os.Remove(sstPath); err != nil {
				log.Logger.Errorf("remove table %d failed: %v", id, err)
				return err
			}
		}
	}
	return nil
}

func (m *Manifest) ReplayManifestFile(f *os.File) (truncOffset int64, err error) {
	r := &util.BufReader{Reader: bufio.NewReader(f)}

	magic := make([]byte, 8)
	if _, err = io.ReadFull(r, magic); err != nil {
		log.Logger.Errorf("read manifest file failed: %v", err)
		return 0, ErrMagic
	}
	if !bytes.Equal(magic[0:4], MagicText[:]) {
		return 0, ErrMagic
	}
	version := util.BytesToUint32(magic[4:8])
	if version != uint32(MagicVersion) {
		return 0, ErrMagic
	}

	for {
		lenbuf := make([]byte, 4)
		// len
		_, err = io.ReadFull(r, lenbuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// change
				break
			}
			return 0, err
		}
		len := util.BytesToUint32(lenbuf)
		// checksum
		checksum := make([]byte, 4)
		_, err = io.ReadFull(r, checksum)
		if err != nil {
			return 0, err
		}
		// commit
		data := make([]byte, len)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return 0, err
		}
		if util.VerifyCheckSum(data, checksum) {
			return 0, ErrChecksum
		}
		var commit proto.ManifestCommit
		if err = proto.Unmarshal(data, &commit); err != nil {
			return 0, err
		}
		if err = m.applyCommit(&commit); err != nil {
			return 0, err
		}
	}
	return r.Offset, nil
}

func (m *Manifest) flush(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// magic
	buf := make([]byte, 8)
	copy(buf[0:4], MagicText[:])
	copy(buf[4:8], util.Uint32ToBytes(uint32(MagicVersion)))

	// commit
	commit := m.getCommit()
	data, err := proto.Marshal(commit)
	if err != nil {
		f.Close()
		return err
	}

	// len
	buf = append(buf, util.Uint32ToBytes(uint32(len(data)))...)
	// checksum
	checksum := util.Checksum(data)
	buf = append(buf, util.Uint32ToBytes(checksum)...)
	// commit
	buf = append(buf, data...)

	if _, err := f.Write(buf); err != nil {
		f.Close()
		log.Logger.Errorf("write manifest file failed: %v", err)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		log.Logger.Errorf("sync manifest file failed: %v", err)
		return err
	}
	return nil
}

func (m *Manifest) applyCommit(commit *proto.ManifestCommit) error {
	for _, change := range commit.Changes {
		if err := m.applyChange(change); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manifest) applyChange(change *proto.ManifestChange) error {
	switch change.Op {
	case proto.ManifestChange_CREATE:
		if _, ok := m.Tables[change.ID]; ok {
			return ErrTableExists
		}
		m.Tables[change.ID] = &TableManifest{
			Level:    uint8(change.Level),
			Checksum: append([]byte{}, change.Checksum...),
		}
		for len(m.Levels) <= int(change.Level) {
			m.Levels = append(m.Levels, &LevelManifest{Tables: make(map[uint64]struct{})})
		}
		m.Levels[change.Level].Tables[change.ID] = struct{}{}
		m.CreateCount++
	case proto.ManifestChange_DELETE:
		if _, ok := m.Tables[change.ID]; !ok {
			return ErrTableNotExists
		}
		delete(m.Tables, change.ID)
		delete(m.Levels[change.Level].Tables, change.ID)
		m.DeleteCount++
	default:
		return ErrInvalidOp
	}
	return nil
}

func (m *Manifest) getCommit() *proto.ManifestCommit {
	var commit proto.ManifestCommit
	commit.Changes = m.getChanges()
	return &commit
}

func (m *Manifest) getChanges() []*proto.ManifestChange {
	changes := make([]*proto.ManifestChange, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newManifestChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}

func newManifestChange(id uint64, level int, checksum []byte) *proto.ManifestChange {
	return &proto.ManifestChange{
		ID:       id,
		Level:    uint32(level),
		Checksum: checksum,
	}
}
