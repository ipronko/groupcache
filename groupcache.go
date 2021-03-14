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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ipronko/groupcache/cache"
	//"github.com/ipronko/groupcache/lru"
	"github.com/ipronko/groupcache/singleflight"
	"github.com/ipronko/groupcache/view"
)

var logger *logrus.Entry

func SetLogger(log *logrus.Entry) {
	logger = log
}

const (
	groupcacheSubdir = "groupcache"
)

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

type CombinedArgs struct {
	MemOpts  cache.Options
	FileOpts cache.FileOptions
}

func NewCombined(name string, memorySize, fileSize int64, getter Getter, memOpts cache.Options, fileOpts cache.FileOptions) (*Group, error) {
	fileGroup, err := NewFile(fmt.Sprintf("%s_%s", name, "file"), fileSize, getter, fileOpts)
	if err != nil {
		return nil, err
	}

	memGroup, err := NewMemory(fmt.Sprintf("%s_%s", name, "memory"), memorySize, fileGroup, memOpts)
	if err != nil {
		return nil, err
	}

	return memGroup, nil
}

func NewMemory(name string, cacheBytes int64, getter Getter, cacheOpts cache.Options) (*Group, error) {
	main, err := cache.NewMemory(cacheBytes*7/8, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating main cache err: %w", err)
	}

	hot, err := cache.NewMemory(cacheBytes/8, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating hot cache err: %w", err)
	}

	return newGroup(name, getter, nil, main, hot)
}

func NewFile(name string, cacheBytes int64, getter Getter, cacheOpts cache.FileOptions) (*Group, error) {
	originRoot := cacheOpts.RootPath
	if originRoot == "" {
		originRoot = os.TempDir()
	}

	cacheOpts.RootPath = filepath.Join(originRoot, groupcacheSubdir, "main")
	main, err := cache.NewFile(cacheBytes*7/8, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating main cache err: %w", err)
	}

	cacheOpts.RootPath = filepath.Join(originRoot, groupcacheSubdir, "hot")
	hot, err := cache.NewFile(cacheBytes/8, cacheOpts)
	if err != nil {
		return nil, fmt.Errorf("creating hot cache err: %w", err)
	}

	return newGroup(name, getter, nil, main, hot)
}

// DeregisterGroup removes group from group pool
func DeregisterGroup(name string) {
	mu.Lock()
	delete(groups, name)
	mu.Unlock()
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, getter Getter, peers PeerPicker, main, hot cache.ViewCache) (*Group, error) {
	if getter == nil {
		return nil, fmt.Errorf("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()

	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		return nil, fmt.Errorf("duplicate registration of group " + name)
	}

	g := &Group{
		name:        name,
		getter:      getter,
		mainCache:   main,
		peers:       peers,
		hotCache:    hot,
		loadGroup:   &singleflight.Group{},
		removeGroup: &singleflight.Group{},
	}
	if fn := newGroupHook; fn != nil {
		fn(g)
	}
	groups[name] = g
	return g, nil
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
	name      string
	getter    Getter
	peersOnce sync.Once
	peers     PeerPicker

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// peer number.
	mainCache cache.ViewCache

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hotspotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache.ViewCache

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
	v, cacheHit := g.lookupCache(key)

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return v, nil
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

	g.mainCache.Add(key, v)
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

	g.hotCache.Add(key, v)
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
	view, ok = g.mainCache.Get(key)
	if ok {
		return view, ok
	}
	return g.hotCache.Get(key)
}

func (g *Group) localRemove(key string) {
	// Ensure no requests are in flight
	g.loadGroup.Lock(func() {
		g.hotCache.Remove(key)
		g.mainCache.Remove(key)
	})
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
func (g *Group) CacheStats(which CacheType) cache.CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.Stats()
	case HotCache:
		return g.hotCache.Stats()
	default:
		return cache.CacheStats{}
	}
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
