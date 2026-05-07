package cache

import "github.com/ipronko/groupcache/view"

// NewNoop returns a ViewCache that discards every write and misses every read.
// Useful for disabling the hot tier when the kernel page cache (or just the
// main file cache) is sufficient.
func NewNoop() ViewCache { return noopCache{} }

type noopCache struct{}

func (noopCache) Add(string, *view.View)               {}
func (noopCache) AddForce(string, *view.View)          {}
func (noopCache) Get(string) (*view.View, bool)        { return nil, false }
func (noopCache) Remove(string)                        {}
func (noopCache) Stats() CacheStats                    { return CacheStats{} }
