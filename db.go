package infinidb

import (
	"github.com/Zaire404/InfiniDB/config"
	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/log"
	"github.com/Zaire404/InfiniDB/lsm"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/pkg/errors"
)

type DB struct {
	opt  *Options
	lsm  *lsm.LSM
	vlog *valueLog
}

func Open(opt *Options) *DB {
	db := &DB{
		opt: opt,
	}
	config.Init()
	log.Init()
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:                      opt.WorkDir,
		MemTableSize:                 opt.MemTableSize,
		SSTableSize:                  opt.SSTableSize,
		BlockSize:                    opt.BlockSize,
		BloomFilterFalsePositiveRate: opt.BloomFilterFalsePositiveRate,
		BaseLevelSize:                opt.BaseLevelSize,
		LevelSizeMultiplier:          opt.LevelSizeMultiplier,
		BaseTableSize:                opt.BaseTableSize,
		NumLevelZeroTables:           opt.NumLevelZeroTables,
		LevelCount:                   uint16(opt.NumLevelZeroTables),
		CompactThreadCount:           uint16(opt.CompactThreadCount),
	})
	db.initVLog()
	// go db.lsm.StartCompact()
	return db
}

func (db *DB) Set(entry *util.Entry) error {
	if entry == nil || len(entry.Key) == 0 {
		return ErrEmptyKey
	}
	if !db.shouldWriteValueToLSM(entry) {
		vp, err := db.vlog.newValuePtr(entry)
		if err != nil {
			return errors.Wrapf(err, "failed to create value pointer for key: %s", string(entry.Key))
		}
		entry.ValueStruct.Meta |= util.BitValuePointer
		entry.ValueStruct.Value = vp.Encode()
	}
	return db.lsm.Set(entry)
}

func (db *DB) Get(key []byte) (*util.Entry, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	entry, err := db.lsm.Get(key)
	if err == nil && entry.ValueStruct.Value == nil {
		return nil, ErrKeyNotFound
	}
	if util.IsValuePtr(entry) {
		vp := &util.ValuePtr{}
		// log.Logger.Debugf("Get key: %s, entry: %v, err: %v", string(key), entry, err)
		vp.Decode(entry.ValueStruct.Value)
		// log.Logger.Debugf("%s", vp.String())
		entry, err = db.vlog.readValuePtr(vp)
	}

	return entry, err
}

func (db *DB) Del(key []byte) error {
	return db.Set(&util.Entry{
		Key: key,
		ValueStruct: util.ValueStruct{
			Value:    nil,
			ExpireAt: 0,
		},
	})
}

func (db *DB) GC() {
	go db.lsm.AutoDelete()
}

func (db *DB) Close() {
	db.lsm.Close()
}

func (db *DB) initVLog() {
	vlog := &valueLog{
		db:  db,
		opt: db.opt,
	}
	vlog.load(db.replayFunction())
	db.vlog = vlog
}

func (db *DB) shouldWriteValueToLSM(e *util.Entry) bool {
	return int64(len(e.ValueStruct.Value)) < db.opt.ValueThreshold
}

func (db *DB) replayFunction() func(*util.Entry, *util.ValuePtr) error {
	return func(entry *util.Entry, vp *util.ValuePtr) error {
		key := make([]byte, len(entry.Key))
		copy(key, entry.Key)
		var val []byte
		meta := entry.ValueStruct.Meta
		if db.shouldWriteValueToLSM(entry) {
			val = make([]byte, len(entry.ValueStruct.Value))
			copy(val, entry.ValueStruct.Value)
		} else {
			val = vp.Encode()
			meta = meta | util.BitValuePointer
		}
		// db.updateHead([]*util.ValuePtr{vp})
		vs := util.ValueStruct{
			Value: val,
			Meta:  meta,
		}
		return db.lsm.Set(&util.Entry{
			Key:         key,
			ValueStruct: vs,
		})
	}
}
