package cache

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/oxtoacart/bpool"

	"github.com/ipronko/groupcache/view"
)

func NewMemory(maxSize int64, opts Options) (*memory, error) {
	opts.Complete(maxSize)

	br := newByteResolver()
	c := &memory{
		maxInstanceSize: opts.MaxInstanceSize,
		data:            br,
		bufPool:         bpool.NewBytePool(opts.CopyBufferSize, opts.CopyBufferWidth),
		logger:          opts.Logger,
	}

	rCache, err := getCache(maxSize, opts, br.evict)
	if err != nil {
		return nil, err
	}

	c.cache = rCache

	return c, nil
}

type Options struct {
	MaxInstanceSize     int64
	NumCacheCounters    int64
	DisableCacheMetrics bool
	Logger              Logger
	CopyBufferSize      int
	CopyBufferWidth     int
}

func (o *Options) Complete(maxSize int64) {
	if o.MaxInstanceSize == 0 {
		o.MaxInstanceSize = int64(defaultMaxInstance)
	}
	if o.NumCacheCounters == 0 {
		o.NumCacheCounters = maxSize / int64(defaultInstanceSize) * 10
	}
	if o.Logger == nil {
		o.Logger = nopLogger{}
	}
	if o.CopyBufferSize == 0 {
		o.CopyBufferSize = defaultBufferInstances
	}
	if o.CopyBufferWidth == 0 {
		o.CopyBufferWidth = int(defaultBuffer)
	}
}

type StoreType int

// cache is a wrapper around an *ristretto.Cache
type memory struct {
	bufPool         *bpool.BytePool
	data            *byteResolver
	maxInstanceSize int64
	logger          Logger
	cache           *ristretto.Cache
}

func (c *memory) Stats() CacheStats {
	return CacheStats{
		Bytes:     c.cache.Metrics.CostAdded() - c.cache.Metrics.CostEvicted(),
		Items:     c.cache.Metrics.KeysAdded() - c.cache.Metrics.KeysEvicted(),
		Gets:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Hits:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Evictions: c.cache.Metrics.KeysEvicted(),
	}
}

func (c *memory) Add(key string, value *view.View) error {
	if buf, ok := value.BytesBuffer(); ok {
		if c.cache.Set(key, byteValue{key: key}, int64(buf.Len())) {
			c.data.add(key, buf.Bytes())
		}
		return nil
	}

	return c.set(key, value)
}

func (c *memory) AddForce(key string, value *view.View) error {
	defer value.Close()

	if buf, ok := value.BytesBuffer(); ok {
		c.setValue(key, buf.Bytes(), int64(buf.Len()), true)
		return nil
	}

	return c.readAndSet(key, value, true)
}

func (c *memory) setValue(key string, val []byte, len int64, force bool) {
	for i := 0; i < 1000; i++ {
		if c.cache.Set(key, byteValue{key: key}, len) {
			c.data.add(key, val)
			return
		}
		if !force {
			return
		}
	}
}

func (c *memory) set(key string, value *view.View) error {
	pipeR, pipeW := io.Pipe()
	oldReader := value.SwapReader(pipeR)
	teeReader := io.TeeReader(oldReader, pipeW)

	go func() {
		defer func() {
			pipeW.Close()
			if rc, ok := oldReader.(io.Closer); ok {
				rc.Close()
			}
		}()

		err := c.readAndSet(key, teeReader, false)
		if err != nil {
			c.logger.Errorf("read and set err: %s", err.Error())
			return
		}
	}()
	return nil
}

const bufferInitCapacity = 10 * 1024 * 1024

func (c *memory) readAndSet(key string, r io.Reader, force bool) error {
	buffPool := c.bufPool.Get()
	defer c.bufPool.Put(buffPool)

	buff := bytes.NewBuffer(make([]byte, 0, bufferInitCapacity))

	wrote, err := io.CopyBuffer(buff, io.LimitReader(r, c.maxInstanceSize), buffPool)
	if err != nil {
		return fmt.Errorf("copy from reader to bytes buffer err: %s", err.Error())
	}

	// discard all data if limit was increased
	if wrote >= c.maxInstanceSize {
		buff = nil
		io.CopyBuffer(ioutil.Discard, r, buffPool)
		return nil
	}

	b := make([]byte, buff.Len())
	copy(b, buff.Bytes())
	c.setValue(key, b, wrote, force)

	return nil
}

func (c *memory) Get(key string) (*view.View, bool) {
	_, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}

	val, ok := c.data.get(key)
	if !ok {
		c.Remove(key)
		return nil, false
	}

	return view.NewView(bytes.NewBuffer(val)), true
}

func (c *memory) Remove(key string) {
	c.cache.Del(key)
	c.data.delete(key)
}

const buckets = 8

type byteValue struct {
	key string
}

func newByteResolver() *byteResolver {
	br := &byteResolver{buckets: make([]*bucket, buckets)}
	for i := 0; i < buckets; i++ {
		br.buckets[i] = &bucket{data: make(map[string][]byte)}
	}
	return br
}

type byteResolver struct {
	buckets []*bucket
}

type bucket struct {
	sync.RWMutex
	data map[string][]byte
}

func (b *bucket) get(key string) ([]byte, bool) {
	b.RLock()
	val, ok := b.data[key]
	b.RUnlock()
	return val, ok
}

func (b *bucket) delete(key string) {
	b.Lock()
	delete(b.data, key)
	b.Unlock()
}

func (b *bucket) add(key string, value []byte) {
	b.Lock()
	b.data[key] = value
	b.Unlock()
}

func (br *byteResolver) add(key string, value []byte) {
	br.getBucket(key).add(key, value)
}

func (br *byteResolver) get(key string) ([]byte, bool) {
	return br.getBucket(key).get(key)
}

func (br *byteResolver) delete(key string) {
	br.getBucket(key).delete(key)
}

func (br *byteResolver) evict(value interface{}) {
	val, ok := value.(byteValue)
	if !ok {
		return
	}
	br.delete(val.key)
}

func (br *byteResolver) getBucket(key string) *bucket {
	return br.buckets[hashNumber(key)%len(br.buckets)]
}

func hashNumber(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}
