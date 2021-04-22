///*
//Copyright 2012 Google Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
//// Tests for groupcache.
//
package groupcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/ipronko/groupcache/cache"
	"github.com/ipronko/groupcache/view"
)

var (
	once                   sync.Once
	fileGroup, memoryGroup Getter

	stringc = make(chan string)

	dummyCtx context.Context

	// cacheFills is the number of times memoryGroup or
	// protoGroup's Getter have been called. Read using the
	// cacheFills function.
	cacheFills AtomicInt
)

const (
	stringGroupName = "memory-group"
	fileGroupName   = "file-group"
	fromChan        = "from-chan"
	cacheSize       = 1 << 20
)

func testSetup() {
	var err error
	memoryGroup, err = NewMemory(stringGroupName, cacheSize, GetterFunc(func(_ context.Context, key string) (*view.View, error) {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		buf := bytes.NewBuffer([]byte("ECHO:" + key))
		return view.NewView(ioutil.NopCloser(buf), int64(buf.Len()), 0), nil
	}), cache.Options{})
	if err != nil {
		panic(err)
	}

	dir, err := ioutil.TempDir("/tmp", "groupcache")
	if err != nil {
		panic(err)
	}
	skipFirst := 0
	fileGroup, err = NewFile(fileGroupName, cacheSize, GetterFunc(func(_ context.Context, key string) (*view.View, error) {
		cacheFills.Add(1)
		buf := bytes.NewBuffer([]byte("ECHO:" + key))
		return view.NewView(ioutil.NopCloser(buf), int64(buf.Len()), time.Millisecond*100), nil
	}), cache.FileOptions{
		Options:        cache.Options{},
		SkipFirstCalls: &skipFirst,
		RootPath:       dir,
	})
	if err != nil {
		panic(err)
	}
}

func countFills(f func()) int64 {
	fills0 := cacheFills.Get()
	f()
	return cacheFills.Get() - fills0
}

func TestCaching(t *testing.T) {
	once.Do(testSetup)
	fills := countFills(func() {
		for i := 0; i < 10; i++ {
			// Need some time to set cache
			if i == 1 {
				time.Sleep(time.Millisecond * 10)
			}
			if _, err := memoryGroup.Get(dummyCtx, "TestCaching-key"); err != nil {
				t.Fatal(err)
			}
		}
	})
	if fills != 1 {
		t.Errorf("expected 1 cache fill; got %d", fills)
	}
}

func TestCachingExpire(t *testing.T) {
	once.Do(testSetup)
	fills := countFills(func() {
		for i := 0; i < 3; i++ {
			if i == 1 {
				time.Sleep(time.Millisecond * 150)
			}
			v, err := fileGroup.Get(dummyCtx, "TestCachingExpire-key")
			if err != nil {
				t.Fatal(err)
			}
			n, err := io.Copy(ioutil.Discard, v)
			if err != nil {
				log.Fatal(err)
			}
			if n != v.Len() {
				t.Fatalf("copied %d bytes, must be %d", n, v.Len())
			}

		}
	})
	if fills != 2 {
		t.Errorf("expected 2 cache fill, got %d", fills)
	}
}

// not stable
func TestCacheEviction(t *testing.T) {
	once.Do(testSetup)
	testKey := "TestCacheEviction-key"
	getTestKey := func() {
		for i := 0; i < 10; i++ {
			if i == 1 {
				time.Sleep(time.Second)
			}
			v, err := memoryGroup.Get(dummyCtx, testKey)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(ioutil.Discard, v)
		}
	}
	fills := countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill; got %d", fills)
	}

	g := memoryGroup.(*Group)
	evict0 := g.mainCache.Stats().Evictions

	// Trash the cache with other keys.
	var bytesFlooded int64
	// cacheSize/len(testKey) is approximate
	for bytesFlooded < cacheSize*2 {
		key := fmt.Sprintf("dummy-key-%d", bytesFlooded)
		v, err := memoryGroup.Get(dummyCtx, key)
		if err != nil {
			t.Fatal(err)
		}
		bytesFlooded += v.Len()
	}
	time.Sleep(10 * time.Millisecond)
	evicts := g.mainCache.Stats().Evictions - evict0
	if evicts <= 0 {
		t.Errorf("evicts = %v; want more than 0", evicts)
	}

	time.Sleep(10 * time.Millisecond)
	// Test that the key is gone.
	fills = countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill after cache trashing; got %d", fills)
	}
}

type fakePeer struct {
	hits int
	fail bool
}

func (p *fakePeer) Get(_ context.Context, in *GetRequest) (*view.View, error) {
	p.hits++
	if p.fail {
		return nil, errors.New("simulated error from peer")
	}
	buf := bytes.NewBuffer([]byte("got:" + in.Key))
	rc := ioutil.NopCloser(buf)
	rView := view.NewView(rc, int64(buf.Len()), 0)
	return rView, nil
}

func (p *fakePeer) Remove(_ context.Context, in *GetRequest) error {
	p.hits++
	if p.fail {
		return errors.New("simulated error from peer")
	}
	return nil
}

func (p *fakePeer) GetURL() string {
	return "fakePeer"
}

type fakePeers []ProtoGetter

func (p fakePeers) PickPeer(key string) (peer ProtoGetter, ok bool) {
	if len(p) == 0 {
		return
	}
	n := crc32.Checksum([]byte(key), crc32.IEEETable) % uint32(len(p))
	return p[n], p[n] != nil
}

func (p fakePeers) GetAll() []ProtoGetter {
	return p
}

// tests that peers (virtual, in-process) are hit, and how much.
func TestPeers(t *testing.T) {
	once.Do(testSetup)
	peer0 := &fakePeer{}
	peer1 := &fakePeer{}
	peer2 := &fakePeer{}
	peerList := fakePeers([]ProtoGetter{peer0, peer1, peer2, nil})
	const cacheSize = 0 // disabled
	localHits := 0
	getter := func(ctx context.Context, key string) (*view.View, error) {
		localHits++
		buf := bytes.NewBuffer([]byte("got:" + key))
		rc := ioutil.NopCloser(buf)
		return view.NewView(rc, int64(buf.Len()), 0), nil
	}

	testGroup, err := newGroup("TestPeers-group", GetterFunc(getter), peerList, nil, nil)
	if err != nil {
		panic(err)
	}
	run := func(name string, n int, wantSummary string) {
		// Reset counters
		localHits = 0
		for _, p := range []*fakePeer{peer0, peer1, peer2} {
			p.hits = 0
		}

		for i := 0; i < n; i++ {
			key := fmt.Sprintf("key-%d", i)
			want := "got:" + key

			v, err := testGroup.Get(dummyCtx, key)
			if err != nil {
				t.Errorf("%s: error on key %q: %v", name, key, err)
				continue
			}

			got, err := ioutil.ReadAll(v)
			if err != nil {
				t.Errorf("%s: error read key %q: %v", name, key, err)
				continue
			}

			if string(got) != want {
				t.Errorf("%s: for key %q, got %q; want %q", name, key, got, want)
			}
		}
		summary := func() string {
			return fmt.Sprintf("localHits = %d, peers = %d %d %d", localHits, peer0.hits, peer1.hits, peer2.hits)
		}
		if got := summary(); got != wantSummary {
			t.Errorf("%s: got %q; want %q", name, got, wantSummary)
		}
	}
	resetCacheSize := func(maxBytes int64) {
		g := testGroup
		mCache, err := cache.NewMemory(1024*1024, cache.Options{})
		if err != nil {
			panic(err)
		}
		g.mainCache = mCache

		hCache, err := cache.NewMemory(1024*1024, cache.Options{})
		if err != nil {
			panic(err)
		}
		g.hotCache = hCache
	}

	// Base case; peers all up, with no problems.
	resetCacheSize(1 << 20)
	run("base", 200, "localHits = 49, peers = 51 49 51")

	// Verify cache was hit.  All localHits and peers are gone as the hotCache has
	// the data we need
	run("cached_base", 200, "localHits = 0, peers = 0 0 0")
	resetCacheSize(0)

	// With one of the peers being down.
	// TODO(bradfitz): on a peer number being unavailable, the
	// consistent hashing should maybe keep trying others to
	// spread the load out. Currently it fails back to local
	// execution if the first consistent-hash slot is unavailable.
	peerList[0] = nil
	run("one_peer_down", 200, "localHits = 100, peers = 0 49 51")

	resetCacheSize(0)
	// Failing peer
	peerList[0] = peer0
	peer0.fail = true
	run("peer0_failing", 200, "localHits = 100, peers = 51 49 51")
}

func TestGroupStatsAlignment(t *testing.T) {
	var g Group
	off := unsafe.Offsetof(g.Stats)
	if off%8 != 0 {
		t.Fatal("Stats structure is not 8-byte aligned.")
	}
}

type slowPeer struct {
	fakePeer
}

func (p *slowPeer) Get(_ context.Context, in *GetRequest) (*view.View, error) {
	time.Sleep(time.Second)
	data := []byte("got:" + in.Key)
	return view.NewView(ioutil.NopCloser(bytes.NewBuffer(data)), int64(len(data)), 0), nil
}

func TestContextDeadlineOnPeer(t *testing.T) {
	once.Do(testSetup)
	peer0 := &slowPeer{}
	peer1 := &slowPeer{}
	peer2 := &slowPeer{}
	peerList := fakePeers([]ProtoGetter{peer0, peer1, peer2, nil})
	getter := func(_ context.Context, key string) (*view.View, error) {
		data := []byte("got:" + key)
		return view.NewView(ioutil.NopCloser(bytes.NewBuffer(data)), int64(len(data)), 0), nil
	}

	cache, err := cache.NewMemory(1024*1024, cache.Options{})
	if err != nil {
		panic(err)
	}
	testGroup, err := newGroup("TestContextDeadlineOnPeer-group", GetterFunc(getter), peerList, cache, cache)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
	defer cancel()

	_, err = testGroup.Get(ctx, "test-key")
	if err != nil {
		if err != context.DeadlineExceeded {
			t.Errorf("expected Get to return context deadline exceeded")
		}
	}
}
