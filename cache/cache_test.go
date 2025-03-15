package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheBasicCRUD(t *testing.T) {
	const n = 100
	cache := NewCache(n)
	wlruSize := n * wlruPct / 100
	slruSize := n - wlruSize
	probationSize := slruSize * slruProbationPct / 100
	// protectedSize := slruSize - probationSize
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set %s: %s\n", key, cache)
	}

	// check the wlru
	for i := n - wlruSize; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		assert.True(t, ok)
		assert.Equal(t, val, res)
		fmt.Printf("get %s: %s\n", key, cache)
	}

	// check the probation and move to protected
	for i := n - wlruSize - probationSize; i < n-wlruSize; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		assert.True(t, ok)
		assert.Equal(t, val, res)
		fmt.Printf("get %s: %s\n", key, cache)
	}

	// check the protected
	for i := n - wlruSize - probationSize; i < n-wlruSize; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		assert.True(t, ok)
		assert.Equal(t, val, res)
		fmt.Printf("get %s: %s\n", key, cache)
	}
}
