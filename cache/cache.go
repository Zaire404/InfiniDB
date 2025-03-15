package cache

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	wlruPct          = 1
	slruProbationPct = 20
)

const (
	STAGE_WLRU = iota
	STAGE_PROBATION
	STAGE_PROTECTED
)

type storeItem struct {
	stage    int
	keyHash  uint64
	conflict uint64
	value    interface{}
}

type Cache struct {
	lock      sync.RWMutex
	data      map[uint64]*list.Element
	filter    *BloomFilter
	wlru      *WLRU
	slru      *SLRU
	cmSketch  *CountMinSketch
	total     int
	threshold int
}

func NewCache(size int) *Cache {
	wlruSize := size * wlruPct / 100
	if wlruSize == 0 {
		wlruSize = 1
	}
	slruSize := size - wlruSize
	probationSize := slruSize * slruProbationPct / 100
	protectedSize := slruSize - probationSize
	data := make(map[uint64]*list.Element, size)
	return &Cache{
		data:     data,
		wlru:     NewWLRU(wlruSize, data),
		slru:     NewSLRU(probationSize, protectedSize, data),
		filter:   NewBloomFilter(size, 0.01),
		cmSketch: NewCountMinSketch(4, size),
	}
}

func (c *Cache) set(key, value interface{}) {
	keyHash, conflitHash := c.keyToHash(key)

	item := storeItem{
		stage:    STAGE_WLRU,
		keyHash:  keyHash,
		conflict: conflitHash,
		value:    value,
	}

	evictedItem := c.wlru.add(&item)
	if evictedItem == nil {
		return
	}

	vitem := c.slru.victim()
	if vitem == nil {
		c.slru.add(evictedItem)
		return
	}

	// if the key is not in the filter, the key has never been accessed
	// no need to replace
	if !c.filter.Allow(uint32(evictedItem.keyHash)) {
		return
	}

	// if the key is in the filter, we need to check the count-min sketch
	// to see if the key need to be replaced
	vcount := c.cmSketch.GetEstimate(vitem.keyHash)
	ocount := c.cmSketch.GetEstimate(evictedItem.keyHash)

	// if the count of the victim is larger than the count of the evicted item
	if vcount > ocount {
		return
	}

	// replace the victim with the evicted item
	c.slru.add(evictedItem)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.total++
	if c.total >= c.threshold {
		c.total = 0
		c.cmSketch.Reset()
		c.filter.Reset()
	}

	keyHash, conflictHash := c.keyToHash(key)
	element, ok := c.data[keyHash]
	if !ok {
		fmt.Printf("key %v not found\n", key)
		c.filter.Allow(uint32(keyHash))
		c.cmSketch.Add(keyHash)
		return nil, false
	}

	// 如果找到了key对应的element，获取到对应的item
	item := element.Value.(*storeItem)

	// if the key is not truely in the cache
	if item.conflict != conflictHash {
		c.filter.Allow(uint32(keyHash))
		c.cmSketch.Add(keyHash)
		return nil, false
	}

	// if the key is in the cache, update the count-min sketch
	c.filter.Allow(uint32(keyHash))
	c.cmSketch.Add(item.keyHash)
	val := item.value
	if item.stage == STAGE_WLRU {
		fmt.Println("get from wlru")
		c.wlru.get(element)
	} else {
		fmt.Println("get from slru")
		c.slru.get(element)
	}
	return val, true
}

func (c *Cache) Set(key, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.set(key, value)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.get(key)
}

func (c *Cache) del(key interface{}) bool {
	keyHash, conflictHash := c.keyToHash(key)
	element, ok := c.data[keyHash]
	if !ok {
		return false
	}

	item := element.Value.(*storeItem)
	if conflictHash != 0 && (conflictHash != item.conflict) {
		return false
	}
	// only delete the key from the data, the record in bloomfilter and cmSketch will not be deleted
	delete(c.data, keyHash)
	return true
}

func (c *Cache) Del(key interface{}) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.del(key)
}

func (c *Cache) String() string {
	var s string
	s += "Cache: "
	s += c.wlru.String() + " | " + c.slru.String()
	return s
}
