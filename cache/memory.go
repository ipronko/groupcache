package cache

import (
	"bytes"
	"fmt"
	"io"
	"time"

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
		lilFile:         opts.LittleFile,
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
	LittleFile          int64
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
	if o.LittleFile == 0 {
		o.LittleFile = int64(defaultLittleFile)
	}
}

type StoreType int

// cache is a wrapper around an *ristretto.Cache
type cache struct {
	bufPool         *bpool.BytePool
	maxInstanceSize int64
	lilFile         int64
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

func (c *cache) Add(key string, value *view.View) error {
	if value.Len() > c.maxInstanceSize {
		return nil
	}

	if buf, ok := value.BytesBuffer(); ok {
		c.cache.SetWithTTL(key, byteValue{
			ttl:  value.Expire(),
			data: buf.Bytes(),
		}, int64(buf.Len()), value.Expire())
		return nil
	}

	return c.set(key, value)
}

func (c *cache) set(key string, value *view.View) error {
	if value.Len() <= c.lilFile {
		buff, err := c.readAndSet(key, value, value.Len(), value.Expire())
		if err != nil {
			return err
		}
		value.SwapReader(buff)
		return nil
	}

	pipeR, pipeW := io.Pipe()
	oldReader := value.SwapReader(pipeR)
	teeReader := io.TeeReader(oldReader, pipeW)

	go func() {
		defer func() {
			pipeW.Close()
			if rc, ok := oldReader.(io.ReadCloser); ok {
				rc.Close()
			}
		}()

		_, err := c.readAndSet(key, teeReader, value.Len(), value.Expire())
		if err != nil {
			c.logger.Errorf("read and set err: %s", err.Error())
			return
		}
	}()
	return nil
}

func (c *cache) readAndSet(key string, rc io.Reader, len int64, expire time.Duration) (*bytes.Buffer, error) {
	bullPool := c.bufPool.Get()
	defer c.bufPool.Put(bullPool)

	buff := bytes.NewBuffer(nil)
	wrote, err := io.CopyBuffer(buff, rc, bullPool)
	if err != nil && c.logger != nil {
		return nil, fmt.Errorf("copy from reader to bytes buffer err: %s", err.Error())
	}

	if wrote != len {
		return nil, fmt.Errorf("wrote %d, value size %d", wrote, len)
	}

	data := buff.Bytes()
	c.cache.SetWithTTL(key, byteValue{
		ttl:  expire,
		data: data,
	}, wrote, expire)

	return buff, nil
}

type byteValue struct {
	ttl  time.Duration
	data []byte
}

func (c *cache) Get(key string) (*view.View, bool) {
	vi, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}

	val, ok := vi.(byteValue)
	if !ok {
		c.Remove(key)
		return nil, false
	}

	return view.NewView(bytes.NewBuffer(val.data), int64(len(val.data)), val.ttl), true
}

func (c *cache) Remove(key string) {
	c.cache.Del(key)
}
