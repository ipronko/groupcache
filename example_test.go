package groupcache_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ipronko/groupcache"
	"github.com/ipronko/groupcache/cache"
	"github.com/ipronko/groupcache/view"
)

var testFile = "LICENSE"

func Test_ExampleUsage(t *testing.T) {
	/*
		// Keep track of peers in our cluster and add our instance to the pool `http://localhost:8080`
		pool := groupcache.NewHTTPPoolOpts("http://localhost:8080", &groupcache.HTTPPoolOptions{})

		// Add more peers to the cluster
		//pool.Set("http://peer1:8080", "http://peer2:8080")

		server := http.Server{
			Addr:    "localhost:8080",
			Handler: pool,
		}

		// Start a HTTP server to listen for peer requests from the groupcache
		go func() {
			log.Printf("Serving....\n")
			if err := server.ListenAndServe(); err != nil {
				log.Fatal(err)
			}
		}()
		defer server.Shutdown(context.Background())
	*/

	file := openFile()

	hash := getHash(file)
	check(file.Close())
	size := fileInfo().Size()
	expTime := time.Minute
	getCalls := int64(10)
	cacheHits := int64(9)
	key := "12345"

	// Create a new group cache with a max cache size of 3MB
	group, err := groupcache.NewMemory("users", 3000000, groupcache.GetterFunc(
		func(ctx context.Context, id string) (*view.View, error) {
			if id != key {
				t.Errorf("expected key %s, got %s", key, id)
			}
			f := openFile()
			return view.NewView(f, size, expTime), nil
		},
	), true, cache.Options{})
	if err != nil {
		t.Errorf("create new group")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := int64(0); i < getCalls; i++ {
		// Need some time to set cache after first call
		if i == 1 {
			time.Sleep(time.Millisecond * 10)
		}
		v, err := group.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}

		check(err)
		testHash := getHash(v)
		v.Close()

		if hash != testHash {
			t.Errorf("not expected hash %s", testHash)
			return
		}

		if v.Len() != size {
			t.Errorf("not expected size %d", v.Len())
			return
		}

		if v.Expire() != expTime {
			t.Errorf("not expected expireTime %s", v.Expire().Round(time.Millisecond))
			return
		}
	}

	if group.Stats.Gets.Get() != getCalls {
		t.Errorf("not expected num of get calls %d", group.Stats.Gets.Get())
		return
	}

	if group.Stats.CacheHits.Get() != cacheHits {
		t.Errorf("not expected num of cache hits %d", group.Stats.CacheHits.Get())
		return
	}

	/*
		// Remove the key from the groupcache
		if err := group.Remove(ctx, "12345"); err != nil {
			fmt.Printf("Remove Err: %s\n", err)
			log.Fatal(err)
		}
	*/
}

func openFile() *os.File {
	file, err := os.Open(testFile)
	check(err)
	return file
}

func fileInfo() os.FileInfo {
	stat, err := os.Stat(testFile)
	check(err)
	return stat
}

func getHash(reader io.Reader) string {
	hashWriter := sha1.New()
	_, err := io.Copy(hashWriter, reader)
	check(err)

	sum := hashWriter.Sum(nil)
	return hex.EncodeToString(sum)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
