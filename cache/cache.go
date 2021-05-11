package cache

import (
	"github.com/alecthomas/units"
	"github.com/dgraph-io/ristretto"

	"github.com/ipronko/groupcache/view"
)

type ViewCache interface {
	Add(key string, value *view.View) error
	AddForce(key string, value *view.View) error
	Get(key string) (v *view.View, ok bool)
	Remove(key string)
	Stats() CacheStats
}

type Logger interface {
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
}

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     uint64
	Items     uint64
	Gets      uint64
	Hits      uint64
	Evictions uint64
}

const (
	defaultInstanceSize = 100 * units.KiB
	defaultMaxInstance  = 200 * units.MiB

	defaultBuffer          = 32 * units.KiB
	defaultBufferInstances = 256
)

type nopLogger struct{}

func (l nopLogger) Errorf(_ string, _ ...interface{}) {}
func (l nopLogger) Infof(_ string, _ ...interface{})  {}

func getCache(maxSize int64, opts Options) (*ristretto.Cache, error) {
	rCache := new(ristretto.Cache)

	config := &ristretto.Config{
		NumCounters: opts.NumCacheCounters,
		MaxCost:     maxSize,
		Metrics:     !opts.DisableCacheMetrics,
		BufferItems: 64,
	}

	config.OnEvict = func(key, conflict uint64, value interface{}, cost int64) {
		onEvict(value, opts.Logger)
	}

	rCache, err := ristretto.NewCache(config)
	if err != nil {
		return nil, err
	}

	return rCache, nil
}
