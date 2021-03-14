package popular

import (
	"sync"
	"time"
)

const (
	maxBuckets = 10
)

func New(popularFrom int, ttl time.Duration) *HitStore {
	h := &HitStore{
		rotateDuration: ttl / maxBuckets,
		popularityHits: popularFrom,
		buckets:        []*bucket{newBucket()},
	}

	go h.gc()

	return h
}

type OnDelete func(key string)

type HitStore struct {
	rotateDuration time.Duration
	popularityHits int

	m       sync.RWMutex
	buckets []*bucket
}

func (h *HitStore) addNew() {
	h.m.Lock()
	h.buckets = append(h.buckets, newBucket())
	h.m.Unlock()
}

func (h *HitStore) gc() {
	for {
		<-time.After(h.rotateDuration)
		h.deleteOld()
		h.addNew()
	}
}

func (h *HitStore) deleteOld() {
	if len(h.buckets) == 1 {
		return
	}

	h.m.Lock()
	pop, b := h.buckets[0], h.buckets[1:]
	h.buckets = b
	h.m.Unlock()

	go h.finishBucket(pop)
}

func (h *HitStore) finishBucket(b *bucket) {
	for k := range b.data {
		if h.has(k) {
			continue
		}
	}
	return
}

func (h *HitStore) has(key string) bool {
	h.m.RLock()
	for i := range h.buckets {
		if h.buckets[i].has(key) {
			h.m.RUnlock()
			return true
		}
	}
	h.m.RUnlock()

	return false
}

func (h *HitStore) hit(key string) {
	h.m.Lock()
	h.buckets[len(h.buckets)-1].hit(key)
	h.m.Unlock()
}

func (h *HitStore) IsPopular(key string) bool {
	var popular bool
	var hits int64

	h.hit(key)

	h.m.RLock()
	for i := range h.buckets {
		hits += h.buckets[i].getHits(key)
		if hits >= int64(h.popularityHits) {
			popular = true
			break
		}
	}
	h.m.RUnlock()

	return popular
}

func newBucket() *bucket {
	return &bucket{
		data: make(map[string]int64),
	}
}

type bucket struct {
	data map[string]int64
}

func (b *bucket) hit(key string) {
	b.data[key]++
}

func (b *bucket) has(key string) bool {
	_, ok := b.data[key]
	return ok
}

func (b *bucket) getHits(key string) int64 {
	return b.data[key]
}
