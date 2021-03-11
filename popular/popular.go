package popular

import (
	"context"
	"sync"
	"time"
)

const (
	maxBuckets = 10
)

func New(ctx context.Context, onDelete OnDelete, popularFrom int64, ttl time.Duration) *HitStore {
	h := &HitStore{
		rotateDuration: ttl / maxBuckets,
		popularityHits: popularFrom,
		onDelete:       onDelete,
		buckets:        []*bucket{newBucket()},
	}

	go h.gc(ctx)

	return h
}

type OnDelete func(key string)

type HitStore struct {
	rotateDuration time.Duration
	popularityHits int64
	onDelete       OnDelete

	m       sync.RWMutex
	buckets []*bucket
}

func (h *HitStore) addNew() {
	h.m.Lock()
	h.buckets = append(h.buckets, newBucket())
	h.m.Unlock()
}

func (h *HitStore) gc(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(h.rotateDuration):
		}

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

		h.onDelete(k)
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
		if hits >= h.popularityHits {
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
