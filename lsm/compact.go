package lsm

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Zaire404/InfiniDB/file"
	"github.com/Zaire404/InfiniDB/log"
	"github.com/Zaire404/InfiniDB/proto"
	"github.com/negrel/assert"
)

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	targets      compactionTargets
}

type compactionTargets struct {
	baseLevel int
	levelSize []int64
	fileSize  []int64
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func newCompactStatus(levels int) *compactStatus {
	cs := &compactStatus{
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < levels; i++ {
		cs.levels = append(cs.levels, newLevelCompactStatus())
	}
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func newLevelCompactStatus() *levelCompactStatus {
	return &levelCompactStatus{
		ranges:  make([]keyRange, 0),
		delSize: 0,
	}
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (cs *compactStatus) delSize(l int) int64 {
	if cs.levels[l] == nil {
		fmt.Print("cs.levels[l] is nil")
	}
	return atomic.LoadInt64(&cs.levels[l].delSize)
}

type thisAndNextLevelRLocked struct{}

// compareAndAdd will check whether we can run this compactDef. That it doesn't overlap with any
// other running compaction. If it can be run, it would store this run in the compactStatus state.
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.level
	assert.Truef(tl < len(cs.levels), "Got level %d. Max levels: %d", tl, len(cs.levels))
	thisLevel := cs.levels[cd.thisLevel.level]
	nextLevel := cs.levels[cd.nextLevel.level]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.ID()] = struct{}{}
	}
	return true
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.level
	assert.Truef(tl < len(cs.levels), "Got level %d. Max levels: %d", tl, len(cs.levels))

	thisLevel := cs.levels[cd.thisLevel.level]
	nextLevel := cs.levels[cd.nextLevel.level]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)

	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		log.Logger.Infof("Looking for: %s in this level %d.\n", this, tl)
		log.Logger.Infof("This Level:\n%s\n", thisLevel.debug())
		log.Logger.Infof("Looking for: %s in next level %d.\n", next, cd.nextLevel.level)
		log.Logger.Infof("Next Level:\n%s\n", nextLevel.debug())
		log.Logger.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.ID()]
		assert.True(ok)
		delete(cs.tables, t.ID())
	}
}

type compactDef struct {
	compactorID int
	targets     compactionTargets
	prio        compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*Table
	bot []*Table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.Lock()
	cd.nextLevel.Lock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.Unlock()
	cd.thisLevel.Unlock()
}

var infRange = keyRange{inf: true}

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps.
	if r.isEmpty() {
		return true
	}

	// Empty dst doesn't overlap with anything.
	if dst.isEmpty() {
		return false
	}

	if r.inf || dst.inf {
		return true
	}

	if bytes.Compare(r.left, dst.right) > 0 || bytes.Compare(r.right, dst.left) < 0 {
		return false
	}

	return true
}

func (r *keyRange) extend(kr keyRange) {
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || bytes.Compare(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || bytes.Compare(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

func getKeyRange(tables ...*Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	smallest := tables[0].MinKey()
	biggest := tables[0].MaxKey()
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i].MinKey(), smallest) < 0 {
			smallest = tables[i].MinKey()
		}
		if bytes.Compare(tables[i].MaxKey(), biggest) > 0 {
			biggest = tables[i].MaxKey()
		}
	}

	return keyRange{
		left:  smallest,
		right: biggest,
	}
}

// 根据compactDef创建ManifestchangeSet
func buildCommit(cd *compactDef, tables []*Table) *proto.ManifestCommit {
	changes := []*proto.ManifestChange{}
	for _, table := range tables {
		changes = append(changes, file.NewCreateChange(table.fid, cd.nextLevel.level))
	}
	for _, table := range cd.top {
		changes = append(changes, file.NewDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, file.NewDeleteChange(table.fid))
	}
	return &proto.ManifestCommit{
		Changes: changes,
	}
}
