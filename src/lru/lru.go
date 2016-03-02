// This package provides a simple LRU cache. It is based on the
// LRU implementation in groupcache:
// https://github.com/golang/groupcache/tree/master/lru
// 
// Modified by William Cheung, 10/13/2015
//

package lru

import (
	"container/list"
	"errors"
	"sync"
)

// LRUCache is a thread-safe fixed capacity LRU cache.
type LRUCache struct {
	capacity  int
	evictList *list.List
	items     map[interface{}]*list.Element
	lock      sync.RWMutex
	onEvicted func(key interface{}, value interface{})
}

// entry is used to hold a value in the evictList
type entry struct {
	key   interface{}
	value interface{}
}

// New creates an LRU of the given capacity
func New(capacity int) *LRUCache {
	c, _ := newWithEvict(capacity, nil)
	return c
}

func newWithEvict(capacity int, onEvicted func(key interface{}, value interface{})) (*LRUCache, error) {
	if capacity <= 0 {
		return nil, errors.New("Must provide a positive capacity")
	}
	c := &LRUCache{
		capacity:      capacity,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element, capacity),
		onEvicted: onEvicted,
	}
	return c, nil
}

// Clear is used to completely clear the cache
func (c *LRUCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for k, v := range c.items {
		if c.onEvicted != nil {
			c.onEvicted(k, v.Value.(*entry).value)
		}

		delete(c.items, k)
	}

	c.evictList.Init()
}

// Put adds a value to the cache.  Returns true if an eviction occured.
func (c *LRUCache) Put(key, value interface{}) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return false
	}

	// Add new item
	ent := &entry{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	evict := c.evictList.Len() > c.capacity
	// Verify capacity not exceeded
	if evict {
		c.removeOldest()
	}
	return evict
}

// Get looks up a key's value from the cache.
func (c *LRUCache) Get(key interface{}) (value interface{}, ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry).value, true
	}
	return
}

// Check if a key is in the cache, without updating the recent-ness or deleting it for being stale.
func (c *LRUCache) Contains(key interface{}) (ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, ok = c.items[key]
	return ok
}

// Returns the key value (or undefined if not found) without updating the "recently used"-ness of the key.
// (If you find yourself using this a lot, you might be using the wrong sort of data structure, but there are some use cases where it's handy.)
func (c *LRUCache) Peek(key interface{}) (value interface{}, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if ent, ok := c.items[key]; ok {
		return ent.Value.(*entry).value, true
	}
	return nil, ok
}

// ContainsOrAdd checks if a key is in the cache (without updating the recent-ness or deleting it for being stale),
// if not, adds the value. Returns whether found and whether an eviction occurred.
func (c *LRUCache) ContainsOrAdd(key, value interface{}) (ok, evict bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, ok = c.items[key]
	if ok {
		return true, false
	}

	ent := &entry{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	evict = c.evictList.Len() > c.capacity
	// Verify capacity not exceeded
	if evict {
		c.removeOldest()
	}
	return false, evict
}

// Remove removes the provided key from the cache.
func (c *LRUCache) Remove(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRUCache) RemoveOldest() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.removeOldest()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRUCache) Keys() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()

	keys := make([]interface{}, len(c.items))
	ent := c.evictList.Back()
	i := 0
	for ent != nil {
		keys[i] = ent.Value.(*entry).key
		ent = ent.Prev()
		i++
	}

	return keys
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *LRUCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRUCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
	if c.onEvicted != nil {
		c.onEvicted(kv.key, kv.value)
	}
}
