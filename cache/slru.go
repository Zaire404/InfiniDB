package cache

import (
	"container/list"
	"fmt"
)

type SLRU struct {
	data                       map[uint64]*list.Element
	probation, protected       *list.List
	probationCap, protectedCap int
}

func NewSLRU(probationCap, protectedCap int, data map[uint64]*list.Element) *SLRU {
	return &SLRU{
		data:         data,
		probation:    list.New(),
		protected:    list.New(),
		probationCap: probationCap,
		protectedCap: protectedCap,
	}
}

func (slru *SLRU) Len() int {
	return slru.probation.Len() + slru.protected.Len()
}

func (slru *SLRU) add(item *storeItem) {
	item.stage = STAGE_PROBATION
	if slru.probation.Len() < slru.probationCap {
		slru.data[item.keyHash] = slru.probation.PushFront(item)
		return
	}
	// if probation is full, remove the last element
	lastElement := slru.probation.Back()
	delete(slru.data, lastElement.Value.(*storeItem).keyHash)
	lastElement.Value = item
	slru.data[item.keyHash] = lastElement
	slru.probation.MoveToFront(lastElement)
}

func (slru *SLRU) get(ele *list.Element) *storeItem {
	item := ele.Value.(*storeItem)
	if item.stage == STAGE_PROTECTED {
		slru.protected.MoveToFront(ele)
		return item
	}
	if slru.protected.Len() < slru.protectedCap {
		slru.probation.Remove(ele)
		item.stage = STAGE_PROTECTED
		slru.data[item.keyHash] = slru.protected.PushFront(item)
		return item
	}

	// Swap between probation and protected when protected is full.
	lastProtectedElement := slru.protected.Back()
	lastItem := lastProtectedElement.Value.(*storeItem)
	// Remove both items temporarily from the mapping.
	delete(slru.data, item.keyHash)
	delete(slru.data, lastItem.keyHash)
	// Swap values between probation's element and the last protected element.
	ele.Value = lastItem
	lastProtectedElement.Value = item
	// Reassign the mapping.
	slru.data[lastItem.keyHash] = ele
	slru.data[item.keyHash] = lastProtectedElement

	slru.probation.MoveToFront(ele)
	slru.protected.MoveToFront(lastProtectedElement)
	return item
}

func (slru *SLRU) victim() *storeItem {
	if slru.Len() < slru.probationCap+slru.protectedCap {
		return nil
	}
	if slru.probation.Len() > 0 {
		return slru.probation.Back().Value.(*storeItem)
	}
	if slru.protected.Len() > 0 {
		return slru.protected.Back().Value.(*storeItem)
	}
	return nil
}

func (slru *SLRU) String() string {
	var res string
	for e := slru.probation.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v ", e.Value.(*storeItem).value)
	}
	res += " | "
	for e := slru.protected.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v ", e.Value.(*storeItem).value)
	}
	return res
}
