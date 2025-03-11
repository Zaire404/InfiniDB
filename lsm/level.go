package lsm

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/Zaire404/InfiniDB/error"
	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/log"
	"github.com/Zaire404/InfiniDB/util"
	"github.com/negrel/assert"
)

type levelManager struct {
	lsm          *LSM
	levels       []*levelHandler
	maxFID       uint64
	manifestFile *file.ManifestFile
	cstatus      *compactStatus
	opt          *Options
}

type levelHandler struct {
	sync.RWMutex
	level      int
	tables     []*Table // tables should be sorted by fid in ascending order
	tableCount int64
	totalSize  int64
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{
		opt: opt,
	}
	lm.cstatus = newCompactStatus(int(opt.LevelCount))
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

func newLevelController(levelNumber int) *levelHandler {
	return &levelHandler{
		level: levelNumber,
	}
}

func (lm *levelManager) String() string {
	var res strings.Builder
	for i := 0; i < int(lm.opt.LevelCount); i++ {
		res.WriteString(lm.levels[i].String())
	}
	return res.String()
}

func (lh *levelHandler) String() string {
	var res strings.Builder
	res.WriteString(fmt.Sprintf(" Level%d: ", lh.level))
	for _, t := range lh.tables {
		res.WriteString(fmt.Sprintf("%d, ", t.fid))
	}
	return res.String()
}
func (lm *levelManager) loadManifest() error {
	var err error
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkDir,
	})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, lm.opt.LevelCount)
	for i := 0; i < int(lm.opt.LevelCount); i++ {
		lm.levels[i] = newLevelController(i)
	}
	if err := lm.manifestFile.SyncManifestWithDir(lm.opt.WorkDir); err != nil {
		return err
	}

	manifest := lm.manifestFile.GetManifest()
	for fid, tableInfo := range manifest.Tables {
		if fid > lm.maxFID {
			lm.maxFID = fid
		}
		fileName := util.GenSSTName(fid)
		filePath := lm.opt.WorkDir + "/" + fileName
		t, err := openTable(lm, filePath, nil)
		if err != nil {
			return err
		}
		lm.levels[tableInfo.Level].addTable(t)
	}

	// maybe unnecessary
	for i := 0; i < int(lm.opt.LevelCount); i++ {
		lm.levels[i].Sort()
	}
	return nil
}

// should be called with the lock held.
func (lh *levelHandler) Sort() {
	// This means tables[0] will be the oldest table in the level.
	if lh.level == 0 {
		// For level 0, sort by 'fid' in ascending order.
		// A lower 'fid' indicates an older table.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// For other levels, sort by the minimum key of the table's SST.
		sort.Slice(lh.tables, func(i, j int) bool {
			return bytes.Compare(lh.tables[i].sst.MinKey(), lh.tables[j].sst.MinKey()) < 0
		})
	}
}

func (lm *levelManager) flush(immutable *MemTable) error {
	builder := newTableBuilder(*lm.opt)
	iter := immutable.sl.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		builder.add(iter.Item().Entry())
	}

	nextFID := immutable.wal.FID()
	sstName := util.GenSSTName(nextFID)
	sstPath := lm.opt.WorkDir + "/" + sstName
	table, err := openTable(lm, sstPath, builder)
	if err != nil {
		return err
	}

	// tables is sorted by fid in ascending order
	lm.levels[0].addTable(table)

	if err := lm.manifestFile.AddTable(nextFID, 0); err != nil {
		return err
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (*util.Entry, error) {
	var entry *util.Entry
	var err error
	for i := 0; i < int(lm.opt.LevelCount); i++ {
		if entry, err = lm.levels[i].Get(key); err == nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[int(lm.opt.LevelCount)-1]
}

func (lm *levelManager) close() error {
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) runCompactor(id int, closer *util.Closer) {
	defer closer.Done()

	//Avoid starting too many compact threads simultaneously
	randTimer := time.NewTimer(time.Duration(util.GetRandomInt(1000)) * time.Millisecond)
	select {
	case <-randTimer.C:
	case <-closer.HasBeenClosed():
		randTimer.Stop()
		return
	}

	moveL0toFront := func(prios []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}
	run := func(p compactionPriority) bool {
		log.Logger.Debugf("\n[Compactor: %d] Start compacting level: %d", id, p.level)
		err := lm.doCompact(id, p)
		switch {
		case err == nil:
			return true
		case err == ErrFillTables:
			// pass
		default:
			log.Logger.Warnf("[Compactor: %d] Error while running compaction: %v", id, err)
		}
		return false
	}
	runOnce := func() bool {
		prios := lm.pickCompactPriority()
		if id == 0 {
			// Worker ID zero prefers to compact L0 always.
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			if run(p) {
				log.Logger.Debugf("[Compactor: %d] Compaction for level: %d DONE\n", id, p.level)
				return true
			}
		}

		return false
	}
	ticker := time.NewTicker(5000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			runOnce()
			// just for testing
			// return
		case <-closer.HasBeenClosed():
			return
		}
	}
}

func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	assert.True(l < int(lm.opt.LevelCount)) // Sanity check.
	if p.targets.baseLevel == 0 {
		p.targets = lm.levelTargets()
	}

	cd := compactDef{
		compactorID:  id,
		prio:         p,
		targets:      p.targets,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		cd.nextLevel = lm.levels[p.targets.baseLevel]
		log.Logger.Debugf("[Compactor: %d] Compacting L0 -> L%d", id, p.targets.baseLevel)
		if !lm.fillTablesL0(&cd) {
			log.Logger.Errorf("[Compactor: %d] FillTablesL0 failed", id)
			return ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// We're not compacting the last level so pick the next level.
		if cd.thisLevel.level != int(lm.opt.LevelCount)-1 {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return ErrFillTables
		}
	}
	defer lm.cstatus.delete(cd) // Remove the ranges from compaction status.

	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Logger.Warnf("[Compactor: %d] Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}
	return nil
}

func (lm *levelManager) pickCompactPriority() (prios []compactionPriority) {
	levelCount := lm.opt.LevelCount
	targets := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			targets:  targets,
		}
		prios = append(prios, pri)
	}

	// Add L0 priority based on the number of tables.
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// All other levels use size to calculate priority.
	for i := 1; i < int(levelCount); i++ {
		// Don't consider those tables that are already being compacted right now.
		delSize := lm.cstatus.delSize(i)

		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		addPriority(i, float64(sz)/float64(targets.levelSize[i]))
	}
	// TODO: optimize
	return prios
}

func (lm *levelManager) levelTargets() compactionTargets {
	adjust := func(size int64) int64 {
		if size < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return size
	}

	levelCount := lm.opt.LevelCount
	targets := compactionTargets{
		levelSize: make([]int64, levelCount),
		fileSize:  make([]int64, levelCount),
	}

	// calculate target level size
	size := lm.lastLevel().getTotalSize()
	for i := int(levelCount - 1); i > 0; i-- {
		ltarget := adjust(size)
		targets.levelSize[i] = adjust(size)
		if targets.baseLevel == 0 && ltarget >= lm.opt.BaseLevelSize {
			targets.baseLevel = int(i)
		}
		size = size / int64(lm.opt.LevelSizeMultiplier)
	}

	// calculate target file size
	size = lm.opt.BaseTableSize
	targets.fileSize[0] = int64(lm.opt.MemTableSize)
	for i := 1; i < int(levelCount); i++ {
		if i == 0 {
			// Use MemTableSize for Level 0. Because at Level 0, we stop compactions based on the
			// number of tables, not the size of the level. So, having a 1:1 size ratio between
			// memtable size and the size of L0 files is better than churning out 32 files per
			// memtable (assuming 64MB MemTableSize and 2MB BaseTableSize).
			targets.fileSize[i] = int64(lm.opt.MemTableSize)
		} else if i <= targets.baseLevel {
			targets.fileSize[i] = size
		} else {
			size *= int64(lm.opt.LevelSizeMultiplier)
			targets.fileSize[i] = size
		}
	}

	// Bring the base level down to the last empty level.
	for i := targets.baseLevel + 1; i < int(levelCount)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		targets.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the
	// target size, pick the next level as the base level.
	base := targets.baseLevel
	lh := lm.levels
	if base < len(lh)-1 && lh[base].getTotalSize() == 0 && lh[base+1].getTotalSize() < targets.levelSize[base+1] {
		targets.baseLevel++
	}
	return targets
}

// Merge tables according to the compaction definition.
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.targets.fileSize) == 0 {
		return errors.New("filesizes cannot be zero. targets are not set")
	}
	timeStart := time.Now()
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	assert.True(len(cd.splits) == 0)
	if thisLevel.level == nextLevel.level {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		lm.addSplits(&cd)
	}
	// impossible?
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// DEBUG
	// log.Logger.Debugf("cd.thisRange: left: %s, right: %s", cd.thisRange.left, cd.thisRange.right)
	// log.Logger.Debugf("cd.nextRange: left: %s, right: %s", cd.nextRange.left, cd.nextRange.right)
	// log.Logger.Debugf("splits: %d", len(cd.splits))
	// for i, kr := range cd.splits {
	// 	log.Logger.Debugf("split%d: left: %s, right: %s", i, kr.left, kr.right)
	// }

	// Table should never be moved directly between levels, always be rewritten to allow discarding
	// invalid versions.
	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	commit := buildCommit(&cd, newTables)
	log.Logger.Debugf("Applying commit: %+v", commit)
	if err = lm.manifestFile.ApplyCommit(commit); err != nil {
		log.Logger.Errorf("Error while applying commit: %+v", err)
		return err
	}

	if err = nextLevel.replaceTables(cd.bot, newTables); err != nil {
		log.Logger.Errorf("Error while replacing tables: %+v", err)
		return err
	}

	if err = thisLevel.deleteTables(cd.top); err != nil {
		log.Logger.Errorf("Error while deleting tables: %+v", err)
		return err
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		log.Logger.Infof("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.level, nextLevel.level, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}

	if cd.thisLevel.level != 0 && len(newTables) > 2*lm.opt.LevelSizeMultiplier {
		log.Logger.Debugf("This Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.top), hex.Dump(cd.thisRange.left), hex.Dump(cd.thisRange.right))
		log.Logger.Debugf("Next Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.bot), hex.Dump(cd.nextRange.left), hex.Dump(cd.nextRange.right))
	}
	return nil
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
// level is the level of the tables being compacted.
func (lm *levelManager) compactBuildTables(level int, cd compactDef) ([]*Table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot
	// log.Logger.Debugf("topTables: %d, botTables: %d", len(topTables), len(botTables))
	// log.Logger.Debugf("toptables:")
	// for _, t := range topTables {
	// 	log.Logger.Debugf("topTables: %d, MinKey: %s, MaxKey: %s", t.fid, t.MinKey(), t.MaxKey())
	// }
	// log.Logger.Debugf("botTables:")
	// for _, t := range botTables {
	// 	log.Logger.Debugf("botTables: %d, MinKey: %s, MaxKey: %s", t.fid, t.MinKey(), t.MaxKey())
	// }
	// for _, t := range topTables {
	// 	fmt.Printf("%d,", t.fid)
	// }
	// fmt.Println()
	// for _, t := range botTables {
	// 	fmt.Printf("%d,", t.fid)
	// }
	// fmt.Println()

	iterOption := util.Options{
		IsAsc: true,
	}
	// TODO: NewIterator(opt)
	newIters := func() []util.Iterator {
		var iters []util.Iterator
		if level == 0 {
			topLen := len(topTables)
			// L0 tables are in ascending order of time. So, we need to iterate in reverse.
			for i := topLen - 1; i >= 0; i-- {
				// TODO: special for L0
				iters = append(iters, topTables[i].NewIterator(&iterOption))
				topTables[i].Print()
			}
		} else {
			assert.True(len(topTables) == 1)
			iters = []util.Iterator{topTables[0].NewIterator(nil)}
			topTables[0].Print()
		}
		fmt.Println("concat botTables:")
		conIter := NewConcatIterator(botTables, &iterOption)
		for conIter.Rewind(); conIter.Valid(); conIter.Next() {
			fmt.Printf("%s,", conIter.Item().Entry().Key)
		}
		fmt.Println()
		return append(iters, NewConcatIterator(botTables, &iterOption))
	}

	res := make(chan *Table, 3)
	throttle := util.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := throttle.Do(); err != nil {
			log.Logger.Errorf("cannot start subcompaction: %+v", err)
			return nil, nil, err
		}
		log.Logger.Debugf("kr: left: %s, right: %s", kr.left, kr.right)
		go func(kr keyRange) {
			defer throttle.Done(nil)
			iters := newIters()
			// it := util.NewLoserTree(iters)
			it := NewMergeIterator(iters, false)
			defer it.Close()
			lm.subcompact(it, kr, cd, throttle, res)
		}(kr)
	}

	var newTables []*Table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()
	// Wait for all table builders to finish and also for newTables accumulator to finish.
	err := throttle.Finish()
	close(res)
	// Wait for all the tables to be built.
	wg.Wait()

	if err == nil {
		err = util.SyncDir(lm.opt.WorkDir)
	}
	if err != nil {
		DecrRefs(newTables)
		return nil, nil, err
	}
	// maybe unnecessary
	sort.Slice(newTables, func(i, j int) bool {
		return bytes.Compare(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return DecrRefs(newTables) }, nil
}

func (lm *levelManager) subcompact(iter util.Iterator, kr keyRange, cd compactDef, throttle *util.Throttle, res chan<- *Table) {
	var lastKey []byte
	fillTableBuilder := func(builder *tableBuilder) {
		for ; iter.Valid(); iter.Next() {
			key := iter.Item().Entry().Key
			if bytes.Equal(lastKey, key) {
				continue
			}
			if len(kr.right) > 0 && bytes.Compare(key, kr.right) >= 0 {
				log.Logger.Debugf("break in subcompact in %s", key)
				break
			}
			if builder.IsReachedCapacity() {
				log.Logger.Debugf("break in subcompact in %s", key)
				break
			}
			lastKey = util.SafeCopy(lastKey, key)
			// fmt.Printf("%s,", key)
			builder.add(iter.Item().Entry())
		}
	}

	if len(kr.left) == 0 {
		iter.Rewind()
	} else {
		iter.Seek(kr.left)
	}
	for iter.Valid() {
		key := iter.Item().Entry().Key
		if len(kr.right) > 0 && bytes.Compare(key, kr.right) >= 0 {
			log.Logger.Debugf("break in subcompact")
			break
		}

		builder := newTableBuilder(*lm.opt)
		builder.opt.SSTableSize = uint32(cd.targets.fileSize[cd.nextLevel.level])
		fillTableBuilder(builder)
		if builder.Empty() {
			// TODO: to clear the resources
			builder.Close()
			// Maybe we should break here?
			continue
		}
		if err := throttle.Do(); err != nil {
			log.Logger.Errorf("cannot start subcompaction: %+v", err)
			return
		}
		go func(builder *tableBuilder) {
			defer throttle.Done(nil)
			fid := atomic.AddUint64(&lm.maxFID, 1)
			sstName := util.GenSSTName(fid)
			sstPath := lm.opt.WorkDir + "/" + sstName
			table, err := openTable(lm, sstPath, builder)
			if err != nil {
				log.Logger.Errorf("Error while opening table: %+v", err)
				return
			}
			res <- table
		}(builder)
	}
}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*Table, len(cd.thisLevel.tables))
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.level == int(lm.opt.LevelCount)-1 {
		// TODO: fillMaxLevelTables
		return true
	}

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// If we're already compacting this range, don't do anything.
		if lm.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}
		cd.top = []*Table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		log.Logger.Debugf("Overlapping tables left : %d, right: %d", left, right)

		cd.bot = make([]*Table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*Table{}
			cd.nextRange = cd.thisRange
			if !lm.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if lm.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}
		if !lm.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	opt := lm.opt
	minSize := int64(opt.NumLevelZeroTables) * int64(opt.MemTableSize)
	l0 := lm.levels[0]
	if l0.getTotalSize() < minSize {
		// If we have really small tables, then no point merging them with base
		// level. Instead, just merge them with each other, until we have a
		// minimum size. Even if we can't run an L0 -> L0 compaction, just let
		// it be, don't merge with Lbase.
		log.Logger.Infof("L0 size is less than minSize. Size: %d, minSize: %d", l0.getTotalSize(), minSize)
		log.Logger.Debugf("FillTablesL0ToL0")
		return lm.fillTablesL0ToL0(cd)
	}
	log.Logger.Debugf("FillTablesL0ToLbase")
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	log.Logger.Debugf("Failed to fillTablesL0ToLbase, try to fillTablesL0ToL0")
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorID != 0 {
		// Only compactor zero can work on this.
		return false
	}
	assert.True(cd.thisLevel.level == 0)

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// Because this level and next level are both level 0, we should NOT acquire
	// the read lock twice, because it can result in a deadlock. So, we don't
	// call compactDef.lockLevels, instead locking the level only once and
	// directly here.
	//
	// As per godocs on RWMutex:
	// If a goroutine holds a RWMutex for reading and another goroutine might
	// call Lock, no goroutine should expect to be able to acquire a read lock
	// until the initial read lock is released. In particular, this prohibits
	// recursive read locking. This is to ensure that the lock eventually
	// becomes available; a blocked Lock call excludes new readers from
	// acquiring the lock.
	// log.Logger.Debugf("Trying to getRLock on L0")
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.cstatus.Lock()
	defer lm.cstatus.Unlock()

	top := cd.thisLevel.tables
	var out []*Table
	log.Logger.Debugf("Tables in L0: %d", len(top))
	maxSz := int64(float64(cd.targets.fileSize[0]) * 0.9)
	for _, t := range top {
		if t.Size() >= maxSz {
			// This file is already big, don't include it.
			continue
		}
		if _, beingCompacted := lm.cstatus.tables[t.ID()]; beingCompacted {
			continue
		}
		out = append(out, t)
	}
	log.Logger.Debugf("Tables to be merged in L0: %d", len(out))
	if len(out) < 4 {
		// If we don't have enough tables to merge in L0, don't do it.
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	thisLevel := lm.cstatus.levels[cd.thisLevel.level]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.cstatus.tables[t.ID()] = struct{}{}
	}
	return true
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.level == 0 {
		panic("Base level can't be zero.")
	}
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	if cd.prio.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*Table

	var kr keyRange
	// cd.top[0] is the oldest file. So we start from the oldest file first.
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			break
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
// after the split, the cd.splits will be [[left1, right1], [left2, right2], ...., [leftN, []byte{}]]
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		buf := make([]byte, len(right))
		copy(buf, right)
		skr.right = buf
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			// maxKey is recorded in the nextRange.
			addRange([]byte{})
			return
		}
		// the last entry in the width.
		if i%width == width-1 {
			right := t.MaxKey()
			addRange(right)
		}
	}
}

func (lh *levelHandler) addTable(table *Table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, table)
	atomic.AddInt64(&lh.totalSize, table.Size())
	atomic.AddInt64(&lh.tableCount, 1)
}

func (lh *levelHandler) getTotalSize() int64 {
	return atomic.LoadInt64(&lh.totalSize)
}

func (lh *levelHandler) numTables() int64 {
	return atomic.LoadInt64(&lh.tableCount)
}

func (lh *levelHandler) Get(key []byte) (*util.Entry, error) {
	if lh.level == 0 {
		return lh.searchL0(key)
	} else {
		return lh.serachLn(key)
	}
}

func (lh *levelHandler) searchL0(key []byte) (*util.Entry, error) {
	// tables is sorted by fid in ascending order
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if entry, err := lh.tables[i].Search(key); err == nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lh *levelHandler) serachLn(key []byte) (*util.Entry, error) {
	if lh.tableCount > 0 && (bytes.Compare(key, lh.tables[0].sst.MinKey()) < 0 ||
		bytes.Compare(key, lh.tables[len(lh.tables)-1].sst.MaxKey()) > 0) {
		return nil, ErrKeyNotFound
	}

	// index := sort.Search((int)(lh.tableCount), func(i int) bool {
	// 	return bytes.Compare(key, lh.tables[i].sst.MaxKey()) < 1
	// })
	// if index < int(lh.tableCount) && bytes.Compare(key, lh.tables[index].sst.MinKey()) >= 0 {
	// 	return lh.tables[index].Search(key)
	// }
	for i := len(lh.tables) - 1; i >= 0; i-- {
		// 每个table都会记录各自的minkey和maxkey，如果key在[minKey,maxKey]区间内就会返回
		if bytes.Compare(key, lh.tables[i].sst.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].sst.MaxKey()) < 1 {
			// log.Logger.Debugf("key: %s, tableIndex: %d", key, lh.tables[i].fid)
			return lh.tables[i].Search(key)
		}
	}
	return nil, ErrKeyNotFound
}

func (lh *levelHandler) replaceTables(delTables []*Table, addTables []*Table) error {
	newTables := make([]*Table, 0, len(lh.tables)-len(delTables)+len(addTables))

	delMap := make(map[uint64]struct{})
	for _, t := range delTables {
		t.sst.Delete()
		delMap[t.fid] = struct{}{}
	}
	lh.Lock()
	defer lh.Unlock()
	for _, t := range lh.tables {
		if _, ok := delMap[t.fid]; ok {
			atomic.AddInt64(&lh.totalSize, -t.Size())
			atomic.AddInt64(&lh.tableCount, -1)
			continue
		}
		newTables = append(newTables, t)
	}
	for _, t := range addTables {
		newTables = append(newTables, t)
		atomic.AddInt64(&lh.totalSize, t.Size())
		atomic.AddInt64(&lh.tableCount, 1)
	}
	lh.tables = newTables
	// maybe unnecessary
	lh.Sort()
	return DecrRefs(delTables)
}

func (lh *levelHandler) deleteTables(delTables []*Table) error {
	newTables := make([]*Table, 0, len(lh.tables)-len(delTables))
	delMap := make(map[uint64]struct{})
	for _, t := range delTables {
		t.sst.Delete()
		delMap[t.fid] = struct{}{}
	}
	lh.Lock()
	defer lh.Unlock()
	for _, t := range lh.tables {
		if _, ok := delMap[t.fid]; ok {
			atomic.AddInt64(&lh.totalSize, -t.Size())
			atomic.AddInt64(&lh.tableCount, -1)
			continue
		}
		newTables = append(newTables, t)
	}
	lh.tables = newTables
	return DecrRefs(delTables)
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (s *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(kr.left, s.tables[i].MaxKey()) <= 0
	})
	right := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(kr.right, s.tables[i].MaxKey()) < 0
	})
	return left, right
}

func (lh *levelHandler) close() error {
	for _, t := range lh.tables {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}
