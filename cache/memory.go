package cache

import (
	"bytes"
	"io"

	"github.com/dgraph-io/ristretto"
	"github.com/oxtoacart/bpool"

	"github.com/ipronko/groupcache/view"
)

func NewMemory(maxSize int64, opts Options) (*cache, error) {
	opts.Complete(maxSize)

	rCache, err := getCache(maxSize, opts)
	if err != nil {
		return nil, err
	}

	c := &cache{
		maxInstanceSize: opts.MaxInstanceSize,
		cache:           rCache,
		bufPool:         bpool.NewBytePool(opts.CopyBufferSize, opts.CopyBufferWidth),
		logger:          opts.Logger,
	}

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
type cache struct {
	bufPool         *bpool.BytePool
	maxInstanceSize int64
	logger          Logger
	cache           *ristretto.Cache
}

func (c *cache) Stats() CacheStats {
	return CacheStats{
		Bytes:     c.cache.Metrics.CostAdded() - c.cache.Metrics.CostEvicted(),
		Items:     c.cache.Metrics.KeysAdded() - c.cache.Metrics.KeysEvicted(),
		Gets:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Hits:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Evictions: c.cache.Metrics.KeysEvicted(),
	}
}

func (c *cache) Add(key string, value view.View) {
	if value.Len() > c.maxInstanceSize {
		return
	}

	if value.Type() == view.ByteType {
		c.cache.SetWithTTL(key, value, value.Len(), value.Expire())
		return
	}

	c.readAndSet(key, value.(*view.ReaderView))
	return
}

func (c *cache) readAndSet(key string, value *view.ReaderView) {
	pipeR, pipeW := io.Pipe()
	reader, _ := value.Reader()

	teeReader := io.TeeReader(reader, pipeW)
	go func() {
		defer pipeW.Close()

		bullPool := c.bufPool.Get()
		defer c.bufPool.Put(bullPool)

		buff := bytes.NewBuffer(nil)
		wrote, err := io.CopyBuffer(buff, teeReader, bullPool)
		if err != nil && c.logger != nil {
			c.logger.Errorf("copy from reader to bytes buffer err: %s", err.Error())
			return
		}

		err = reader.Close()
		if err != nil && c.logger != nil {
			c.logger.Errorf("close reader err: %s", err.Error())
			return
		}

		if wrote != value.Len() {
			c.logger.Errorf("wrote %d, value size %d", wrote, value.Len())
			return
		}

		byteView := view.NewByteView(buff.Bytes(), value.Expire())
		c.cache.SetWithTTL(key, byteView, wrote, value.Expire())
	}()

	value.SwapReader(pipeR)
}

func (c *cache) Get(key string) (v view.View, ok bool) {
	vi, ok := c.cache.Get(key)
	if !ok {
		return
	}

	v, ok = vi.(view.View)
	return
}

func (c *cache) Remove(key string) {
	c.cache.Del(key)
}
