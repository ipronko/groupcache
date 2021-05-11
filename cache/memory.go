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

	// TODO del after debug
	go c.printStats()

	return c, nil
}

func (c *memory) printStats() {
	t := time.NewTimer(time.Second * 10)
	defer t.Stop()
	for range t.C {
		c.logger.Infof("memory cache stats: %s", c.cache.Metrics.String())
	}
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
	if buf, ok := value.BytesBuffer(); ok {
		c.cache.Set(key, byteValue{data: buf.Bytes()}, int64(buf.Len()))
		return nil
	}

	return c.set(key, value)
}

func (c *memory) AddForce(key string, value *view.View) error {
	defer value.Close()

	if buf, ok := value.BytesBuffer(); ok {
		c.setValue(key, byteValue{
			data: buf.Bytes(),
		}, int64(buf.Len()), true)
		return nil
	}

	_, err := c.readAndSet(key, value, true)
	return err
}

func (c *memory) setValue(key string, val byteValue, len int64, force bool) {
	for i := 0; i < 1000; i++ {
		if c.cache.Set(key, val, len) {
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

		_, err := c.readAndSet(key, ioutil.NopCloser(teeReader), false)
		if err != nil {
			c.logger.Errorf("read and set err: %s", err.Error())
			return
		}
	}()
	return nil
}

func (c *memory) readAndSet(key string, r io.Reader, force bool) (*bytes.Buffer, error) {
	buffPool := c.bufPool.Get()
	defer c.bufPool.Put(buffPool)

	buff := bytes.NewBuffer(nil)

	wrote, err := io.CopyBuffer(buff, io.LimitReader(r, c.maxInstanceSize), buffPool)
	if err != nil {
		err = fmt.Errorf("copy from reader to bytes buffer err: %s", err.Error())
		c.logger.Errorf(err.Error())
		return nil, err
	}

	// copy all data if limit was increased
	io.CopyBuffer(ioutil.Discard, r, buffPool)

	if wrote >= c.maxInstanceSize {
		return nil, nil
	}

	val := byteValue{
		data: buff.Bytes(),
	}

	c.setValue(key, val, wrote, force)

	return buff, nil
}

type byteValue struct {
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

	return view.NewView(bytes.NewBuffer(val.data)), true
}

func (c *memory) Remove(key string) {
	c.cache.Del(key)
}
