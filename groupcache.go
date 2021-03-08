/*
Copyright 2012 Google Inc.

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

// Package groupcache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package groupcache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ipronko/groupcache/lru"
	"github.com/ipronko/groupcache/singleflight"
	"github.com/ipronko/groupcache/view"
)

var logger *logrus.Entry

func SetLogger(log *logrus.Entry) {
	logger = log
}

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx context.Context, key string) (view.View, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string) (view.View, error)

func (f GetterFunc) Get(ctx context.Context, key string) (view.View, error) {
	return f(ctx, key)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes, maxInstanceMemSize int64, getter Getter) *Group {
	return newGroup(name, cacheBytes, getter, nil, maxInstanceMemSize)
}

// DeregisterGroup removes group from group pool
func DeregisterGroup(name string) {
	mu.Lock()
	delete(groups, name)
	mu.Unlock()
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker, maxMemCacheSize int64) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}
	g := &Group{
		name:        name,
		getter:      getter,
		peers:       peers,
		mainCache:   cache{maxSize: maxMemCacheSize},
		hotCache:    cache{maxSize: maxMemCacheSize},
		cacheBytes:  cacheBytes,
		loadGroup:   &singleflight.Group{},
		removeGroup: &singleflight.Group{},
	}
	if fn := newGroupHook; fn != nil {
		fn(g)
	}
	groups[name] = g
	return g
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart registers a hook that is run when the first
// group is created.
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type Group struct {
	name       string
	getter     Getter
	peersOnce  sync.Once
	peers      PeerPicker
	cacheBytes int64 // limit for sum of mainCache and hotCache size

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// peer number.
	mainCache cache

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hotspotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely), regardless of the number of
	// concurrent callers.
	loadGroup flightGroup

	// removeGroup ensures that each removed key is only removed
	// remotely once regardless of the number of concurrent callers.
	removeGroup flightGroup

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms

	// Stats are statistics on the group.
	Stats Stats
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
	Lock(fn func())
}

// Stats are per-group statistics.
type Stats struct {
	Gets                     AtomicInt // any Get request, including from peers
	CacheHits                AtomicInt // either cache was good
	GetFromPeersLatencyLower AtomicInt // slowest duration to request value from peers
	PeerLoads                AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors               AtomicInt
	Loads                    AtomicInt // (gets - cacheHits)
	LoadsDeduped             AtomicInt // after singleflight
	LocalLoads               AtomicInt // total good local loads
	LocalLoadErrs            AtomicInt // total bad local loads
	ServerRequests           AtomicInt // gets that came over the network from peers
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) initPeers() {
	if g.peers == nil {
		g.peers = getPeers(g.name)
	}
}

func (g *Group) Get(ctx context.Context, key string) (view.View, error) {
	g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(1)
	view, cacheHit := g.lookupCache(key)

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return view, nil
	}

	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	return g.load(ctx, key)
}

// Remove clears the key from our cache then forwards the remove
// request to all peers.
func (g *Group) Remove(ctx context.Context, key string) error {
	g.peersOnce.Do(g.initPeers)

	_, err := g.removeGroup.Do(key, func() (interface{}, error) {

		// Remove from key owner first
		owner, ok := g.peers.PickPeer(key)
		if ok {
			if err := g.removeFromPeer(ctx, owner, key); err != nil {
				return nil, err
			}
		}
		// Remove from our cache next
		g.localRemove(key)
		wg := sync.WaitGroup{}
		errs := make(chan error)

		// Asynchronously clear the key from all hot and main caches of peers
		for _, peer := range g.peers.GetAll() {
			// avoid deleting from owner a second time
			if peer == owner {
				continue
			}

			wg.Add(1)
			go func(peer ProtoGetter) {
				errs <- g.removeFromPeer(ctx, peer, key)
				wg.Done()
			}(peer)
		}
		go func() {
			wg.Wait()
			close(errs)
		}()

		// TODO(thrawn01): Should we report all errors? Reporting context
		//  cancelled error for each peer doesn't make much sense.
		var err error
		for e := range errs {
			err = e
		}

		return nil, err
	})
	return err
}

// load loads key either by invoking the getter locally or by sending it to another machine.
func (g *Group) load(ctx context.Context, key string) (view.View, error) {
	g.Stats.Loads.Add(1)

	if value, cacheHit := g.lookupCache(key); cacheHit {
		g.Stats.CacheHits.Add(1)
		return value, nil
	}

	g.Stats.LoadsDeduped.Add(1)
	var value view.View
	var err error
	if peer, ok := g.peers.PickPeer(key); ok {

		// metrics duration start
		start := time.Now()

		// get value from peers
		value, err = g.getFromPeer(ctx, peer, key)

		// metrics duration compute
		duration := int64(time.Since(start)) / int64(time.Millisecond)

		// metrics only store the slowest duration
		if g.Stats.GetFromPeersLatencyLower.Get() < duration {
			g.Stats.GetFromPeersLatencyLower.Store(duration)
		}

		if err == nil {
			g.Stats.PeerLoads.Add(1)
			return value, nil
		}

		if logger != nil {
			logger.WithFields(logrus.Fields{
				"err":      err,
				"key":      key,
				"category": "groupcache",
			}).Errorf("error retrieving key from peer '%s'", peer.GetURL())
		}

		g.Stats.PeerErrors.Add(1)
		if ctx != nil && ctx.Err() != nil {
			return nil, err
		}
	}

	value, err = g.getLocally(ctx, key)
	if err != nil {
		g.Stats.LocalLoadErrs.Add(1)
		return nil, err
	}
	g.Stats.LocalLoads.Add(1)

	return value, nil
}

func (g *Group) getLocally(ctx context.Context, key string) (view.View, error) {
	v, err := g.getter.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	g.populateCache(key, v, &g.mainCache)
	return v, nil

}

func (g *Group) getFromPeer(ctx context.Context, peer ProtoGetter, key string) (view.View, error) {
	req := &GetRequest{
		Group: g.name,
		Key:   key,
	}

	v, err := peer.Get(ctx, req)
	if err != nil {
		return v, err
	}

	if !v.Expire().IsZero() {
		if time.Now().After(v.Expire()) {
			return v, errors.New("peer returned expired value")
		}
	}

	// Always populate the hot cache
	g.populateCache(key, v, &g.hotCache)
	return v, nil
}

func (g *Group) removeFromPeer(ctx context.Context, peer ProtoGetter, key string) error {
	req := &GetRequest{
		Group: g.name,
		Key:   key,
	}
	return peer.Remove(ctx, req)
}

func (g *Group) lookupCache(key string) (view view.View, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	view, ok = g.mainCache.get(key)
	if ok {
		return view, ok
	}
	return g.hotCache.get(key)
}

func (g *Group) localRemove(key string) {
	// Clear key from our local cache
	if g.cacheBytes <= 0 {
		return
	}

	// Ensure no requests are in flight
	g.loadGroup.Lock(func() {
		g.hotCache.remove(key)
		g.mainCache.remove(key)
	})
}

func (g *Group) populateCache(key string, value view.View, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	cache.add(key, value)

	// Evict items from cache(s) if necessary.
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := &g.mainCache
		if hotBytes > mainBytes/8 {
			victim = &g.hotCache
		}
		victim.removeOldest()
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

// cache is a wrapper around an *lru.Cache that adds synchronization,
// makes values always be ByteView, and counts the size of all keys and
// values.
type cache struct {
	maxSize    int64
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value view.View) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			MaxSize: c.maxSize,
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(view.View)
				size := val.Len()
				c.nbytes -= int64(len(key.(string))) + size
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value, func() {
		c.nbytes += int64(len(key)) + value.Len()
	})
}

func (c *cache) get(key string) (v view.View, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++

	v, ok = vi.(view.View)
	return
}

func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Store atomically stores n to i.
func (i *AtomicInt) Store(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}
