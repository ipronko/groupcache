package cache

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/oxtoacart/bpool"

	"github.com/ipronko/groupcache/view"
)

func NewMemory(maxSize int64, opts Options) (*memory, error) {
	opts.Complete(maxSize)

	rCache, err := getCache(maxSize, opts)
	if err != nil {
		return nil, err
	}

	c := &memory{
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
type memory struct {
	bufPool         *bpool.BytePool
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
	//TODO ignore, use https://github.com/djherbis/buffer buffer.NewSpill
	//if value.Len() > c.maxInstanceSize {
	//	return nil
	//}

	if buf, ok := value.BytesBuffer(); ok {
		c.cache.SetWithTTL(key, byteValue{
			ttl:  value.Expire(),
			data: buf.Bytes(),
		}, int64(buf.Len()), value.Expire())
		return nil
	}

	return c.set(key, value)
}

func (c *memory) AddForce(key string, value *view.View) error {
	defer value.Close()

	//TODO ignore, use https://github.com/djherbis/buffer buffer.NewSpill
	//if value.Len() > c.maxInstanceSize {
	//	return nil
	//}

	if buf, ok := value.BytesBuffer(); ok {
		c.setValue(key, byteValue{
			ttl:  value.Expire(),
			data: buf.Bytes(),
		}, int64(buf.Len()), value.Expire(), true)
		return nil
	}

	_, err := c.readAndSet(key, value, value.Expire(), true)
	return err
}

func (c *memory) setValue(key string, val byteValue, len int64, expire time.Duration, force bool) {
	for i := 0; i < 1000; i++ {
		if c.cache.SetWithTTL(key, val, len, expire) {
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
			if rc, ok := oldReader.(io.ReadCloser); ok {
				rc.Close()
			}
		}()

		_, err := c.readAndSet(key, ioutil.NopCloser(teeReader), value.Expire(), false)
		if err != nil {
			c.logger.Errorf("read and set err: %s", err.Error())
			return
		}
	}()
	return nil
}

func (c *memory) readAndSet(key string, r io.Reader, expire time.Duration, force bool) (*bytes.Buffer, error) {
	bullPool := c.bufPool.Get()
	defer c.bufPool.Put(bullPool)

	buff := bytes.NewBuffer(nil)

	//TODO discard if increase max file limit. https://github.com/djherbis/buffer buffer.NewSpill can be used
	wrote, err := io.CopyBuffer(buff, r, bullPool)
	if err != nil {
		err = fmt.Errorf("copy from reader to bytes buffer err: %s", err.Error())
		c.logger.Errorf(err.Error())
		return nil, err
	}

	val := byteValue{
		ttl:  expire,
		data: buff.Bytes(),
	}

	c.setValue(key, val, wrote, expire, force)

	return buff, nil
}

type byteValue struct {
	ttl  time.Duration
	data []byte
}

func (c *memory) Get(key string) (*view.View, bool) {
	vi, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}

	val, ok := vi.(byteValue)
	if !ok {
		c.Remove(key)
		return nil, false
	}

	return view.NewView(bytes.NewBuffer(val.data), val.ttl), true
}

func (c *memory) Remove(key string) {
	c.cache.Del(key)
}
