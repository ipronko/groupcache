package cache

import (
	"github.com/alecthomas/units"
	"github.com/dgraph-io/ristretto"

	"github.com/ipronko/groupcache/view"
)

// ViewCache stores values keyed by string. Implementations may consume the
// supplied *view.View asynchronously, but the caller retains ownership of the
// value and is responsible for calling value.Close().
type ViewCache interface {
	Add(key string, value *view.View)
	AddForce(key string, value *view.View)
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

// getCache constructs the underlying ristretto cache. evictFunc is invoked
// for every value removal — eviction by the policy, explicit Del, Set
// replacing an existing key, and rejection — so callers can reliably tear
// down side state (e.g. files on disk) keyed by the value.
func getCache(maxSize int64, opts Options, evictFunc func(value interface{})) (*ristretto.Cache, error) {
	config := &ristretto.Config{
		NumCounters: opts.NumCacheCounters,
		MaxCost:     maxSize,
		Metrics:     !opts.DisableCacheMetrics,
		BufferItems: 64,
		OnExit:      evictFunc,
	}

	return ristretto.NewCache(config)
}
