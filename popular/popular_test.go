package popular

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestHitStore_Hit(t *testing.T) {
	store := New(2, time.Minute)
	key := "some"
	if store.IsPopular(key) {
		t.Fatal("fist hit must be not popular")
	}

	if !store.IsPopular(key) {
		t.Fatal("second hit must be popular")
	}
}

func genKey() string {
	i := rand.Int63n(10000000000)
	return fmt.Sprint(i)
}

const (
	benchKeys = 1000000
	ttl       = 10 * time.Second
)

// goos: darwin
// goarch: amd64
// pkg: github.com/ipronko/groupcache/popular
// BenchmarkIsPopular
// BenchmarkIsPopular/IsPopular
// BenchmarkIsPopular/IsPopular-12         	 2267925	      1894 ns/op
func BenchmarkIsPopular(b *testing.B) {
	store := New(2, ttl)

	// Add keys
	for i := 0; i < benchKeys; i++ {
		store.IsPopular(genKey())
	}

	b.StartTimer()

	b.Run("IsPopular", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.IsPopular(genKey())
		}
	})
	b.StopTimer()
}
