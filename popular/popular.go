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
	h.m.Lock()
	defer h.m.Unlock()
	if len(h.buckets) == 1 {
		return
	}
	h.buckets = h.buckets[1:]
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

func (b *bucket) getHits(key string) int64 {
	return b.data[key]
}
