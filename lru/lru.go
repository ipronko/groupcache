/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package lru implements an LRU cache.
package lru

import (
	"bytes"
	"container/list"
	"io"
	"time"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"
	"github.com/sirupsen/logrus"

	"github.com/ipronko/groupcache/view"
)

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	MaxSize int64

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key Key, value interface{})

	ll    *list.List
	cache map[interface{}]*list.Element

	cacheDir string

	//maxMemSize int64
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

type entry struct {
	key    Key
	value  interface{}
	expire time.Time
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key Key, value view.View, onAdd func()) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}

	if value.Len() > c.MaxSize {
		return
	}

	if value.Type() != view.ReaderType {
		c.add(key, value)
		onAdd()
		return
	}

	c.addAfterRead(key.(string), value.(*view.ReaderView), onAdd)
}

func (c *Cache) add(key Key, value view.View) {
	c.cache[key] = &list.Element{Value: &entry{key, value, value.Expire()}}
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}

	c.ll.PushFront(&entry{key, value, value.Expire()})
}

func (c *Cache) addAfterRead(key string, value *view.ReaderView, onAdd func()) {
	c.writeToBuf(key, value, onAdd)
	return
}

func (c *Cache) writeToBuf(key string, value *view.ReaderView, onAdd func()) {
	pipeR, pipeW := nio.Pipe(buffer.New(4 * 1024))
	reader, err := value.Reader()
	if err != nil {
		logrus.Errorf("")
		return
	}

	teeReader := io.TeeReader(reader, pipeW)
	go func() {
		defer pipeW.Close()

		buff := bytes.NewBuffer(nil)
		wrote, err := nio.Copy(buff, teeReader, buffer.New(4*1024))
		if err != nil {
			logrus.Error(err)
			return
		}
		err = reader.Close()
		if err != nil {
			logrus.Error(err)
			return
		}
		if wrote != value.Len() {
			logrus.Errorf("wrote %d, value size %d", wrote, value.Len())
			return
		}

		byteView := view.NewByteView(buff.Bytes(), value.Expire())

		c.add(key, byteView)
		onAdd()
	}()

	value.SwapReader(pipeR)
}

//func (c *Cache) writeToFile(key string, value view.ReaderView, onAdd func())  {
//	file, err := ioutil.TempFile(filepath.Join(c.cacheDir, tmpDir), "cache")
//	if err != nil {
//		return
//	}
//
//	reader, _ := value.Reader()
//
//	pipeR, pipeW := io.Pipe()
//	teeReader := io.TeeReader(reader, pipeW)
//
//	go func() {
//		defer os.Remove(file.Name())
//		written, err := io.Copy(file, pipeR)
//		if err != nil {
//			return
//		}
//
//		if written != value.Len() {
//			return
//		}
//
//		err = c.moveToFiles(file.Name(), key)
//		if err != nil {
//			return
//		}
//
//		c.ll.PushFront(&entry{key, view.NewFileView(c.cachePath(key), value.Len(), value.Expire()), value.Expire()})
//		onAdd()
//	}()
//
//	value.SwapReader(view.NopCloser{Reader: teeReader})
//}

//func (c *Cache) moveToFiles(fileName, key string) error {
//	return os.Rename(fileName, c.cachePath(key))
//}
//
//func (c *Cache) cachePath(key string) string {
//	return filepath.Join(c.cacheDir, filesDir, c.getSuffix(key), key)
//}
//
//func (c *Cache) getSuffix(key string) string {
//	switch len(key) {
//	case 0:
//		return fmt.Sprintf("000")
//	case 1:
//		return fmt.Sprintf("00%s", key)
//	case 2:
//		return fmt.Sprintf("0%s", key)
//	case 3:
//		return key
//	}
//	return key[len(key)-3:]
//}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key Key) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		entry := ele.Value.(*entry)
		// If the entry has expired, remove it from the cache
		if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
			c.removeElement(ele)
			return nil, false
		}

		c.ll.MoveToFront(ele)
		return entry.value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *Cache) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = nil
	c.cache = nil
}
