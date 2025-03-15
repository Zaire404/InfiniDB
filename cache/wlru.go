package cache

import (
	"container/list"
	"fmt"
)

type WLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

func NewWLRU(size int, data map[uint64]*list.Element) *WLRU {
	return &WLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (wlru *WLRU) add(item *storeItem) (evictedItem *storeItem) {
	evictedItem = nil
	if wlru.list.Len() < wlru.cap {
		wlru.data[item.keyHash] = wlru.list.PushFront(item)
		return evictedItem
	}

	// evict the last element
	lastElement := wlru.list.Back()
	evictedItem = lastElement.Value.(*storeItem)
	delete(wlru.data, evictedItem.keyHash)

	// replace the victim with the new item
	lastElement.Value = item
	wlru.data[item.keyHash] = lastElement
	wlru.list.MoveToFront(lastElement)

	return evictedItem
}

func (wlru *WLRU) get(ele *list.Element) *storeItem {
	wlru.list.MoveToFront(ele)
	return ele.Value.(*storeItem)
}

func (wlru *WLRU) String() string {
	var res string
	for e := wlru.list.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	return res
}
