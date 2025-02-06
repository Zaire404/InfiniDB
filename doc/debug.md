## SkipList并发慢
在并发数n从 10,000 增加到 100,000 时，性能显著下降
```
=== RUN   TestConcurrentBasic
--- PASS: TestConcurrentBasic (12.00s)
PASS
ok      github.com/Zaire404/InfiniDB/lsm     12.002s
```
修改前
```go
func (sl *SkipList) Add(e *util.Entry) {
	// Find the insertion point.
	prev := make([]uint32, maxHeight+1)
	next := make([]uint32, maxHeight+1)
	before := sl.headOffset
	for i := int(sl.height) - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, before)
		if prev[i] == next[i] {
			// The key already exists.
			node := getNode(sl.arena, prev[i])
			valueOffset := putValue(sl.arena, e.Value)
			node.value = encodeValue(valueOffset, e.Value.EncodedSize())
			return
		}
	}

	// Insert the new node.
	height := randomHeight()
	newNode := newNode(sl.arena, e.Key, e.Value, uint32(height))
	oldHeight := sl.getHeight()
	for height > oldHeight {
		// CAS to update the height of the skiplist.
		if atomic.CompareAndSwapUint32(&sl.height, oldHeight, height) {
			break
		}
		oldHeight = sl.getHeight()
	}

	newNodeOffset := getNodeOffset(sl.arena, newNode)
	for i := 0; i < int(height); i++ {
		for {
			// CAS is no need here because the newNode is not visible to other goroutines.
			newNode.next[i] = next[i]
			prevNode := getNode(sl.arena, prev[i])
			if prevNode == nil {
				// Height exceeds the old height of the skiplist.
				prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, sl.headOffset)
				prevNode = getNode(sl.arena, prev[i])
			}
			if prevNode.casNextOffset(i, next[i], newNodeOffset) {
				break
			} else {
				// Recompute the prev and next.
				prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, before)
			}
		}
	}

}
```
将两次findSpliceLevel修改
```go
prev[sl.getHeight()] = sl.headOffset
	for i := int(sl.height) - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, prev[i+1])
-----------------------------------------------------------------------
if prevNode.casNextOffset(i, next[i], newNodeOffset) {
	break
} else {
	// Recompute the prev and next.
	prev[i], next[i] = sl.findSpliceForLevel(i, e.Key, prev[i])
}
```