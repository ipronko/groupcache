///*
//Copyright 2013 Google Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
package groupcache

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ipronko/groupcache/cache"
	"github.com/ipronko/groupcache/discovery"
	"github.com/ipronko/groupcache/view"
)

var (
	peerAddrs  = flag.String("test_peer_addrs", "", "Comma-separated list of peer addresses; used by TestHTTPPool")
	peerIndex  = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestHTTPPool")
	peerChild  = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestHTTPPool")
	serverAddr = flag.String("test_server_addr", "", "Address of the server Child Getters will hit ; used by TestHTTPPool")
)

// Works only with local consul
func _TestHTTPPool(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool(t)
		os.Exit(0)
	}

	const (
		nChild = 4
		nGets  = 100
	)

	var serverHits int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello")
		serverHits++
	}))
	defer ts.Close()

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nChild; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestHTTPPool",
			"--test_peer_child",
			"--test_peer_addrs="+strings.Join(childAddr, ","),
			"--test_peer_index="+strconv.Itoa(i),
			"--test_server_addr="+ts.URL,
		)
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, childAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	sd, err := discovery.New("127.0.0.1:8500", "groupcache_test", "main", discovery.WithLogger(logrus.New()), discovery.WithTTL(time.Second))
	if err != nil {
		panic(err)
	}
	_, err = NewHTTPPoolOpts(context.Background(), "ignored", &HTTPPoolOptions{
		ServiceDiscovery: sd,
		Auth:             "auth",
	})
	if err != nil {
		panic(err)
	}
	<-time.After(time.Second * 2)
	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := GetterFunc(func(ctx context.Context, key string) (*view.View, error) {
		return nil, errors.New("parent getter called; something's wrong")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	g, err := NewMemory("httpPoolTest", 1<<20, getter, cache.Options{
		MaxInstanceSize: 1 << 20,
		Logger:          logrus.New(),
	})
	if err != nil {
		panic(err)
	}
	for _, key := range testKeys(nGets) {
		val, err := g.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		data, err := ioutil.ReadAll(val)
		if err != nil {
			panic(err)
		}
		val.Close()

		if suffix := ":" + key; !strings.HasSuffix(string(data), suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, string(data), suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, string(data))
	}

	if serverHits != nGets {
		t.Error("expected serverHits to equal nGets")
	}
	serverHits = 0

	var key = "removeTestKey"

	// Multiple gets on the same key
	for i := 0; i < 2; i++ {
		if _, err := g.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
	}

	// Should result in only 1 server get
	if serverHits != 1 {
		t.Error("expected serverHits to be '1'")
	}

	// Remove the key from the cache and we should see another server hit
	if err := g.Remove(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Get the key again
	if _, err := g.Get(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Should register another server get
	if serverHits != 2 {
		t.Error("expected serverHits to be '2'")
	}
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

func beChildForTestHTTPPool(t *testing.T) {
	addrs := strings.Split(*peerAddrs, ",")

	sd, err := discovery.New("127.0.0.1:8500", "groupcache_test", addrs[*peerIndex], discovery.WithLogger(logrus.New()))
	if err != nil {
		panic(err)
	}

	p, err := NewHTTPPoolOpts(context.Background(), "http://"+addrs[*peerIndex], &HTTPPoolOptions{
		ServiceDiscovery: sd,
		Auth:             "auth",
	})
	if err != nil {
		panic(err)
	}

	getter := GetterFunc(func(ctx context.Context, key string) (*view.View, error) {
		if _, err := http.Get(*serverAddr); err != nil {
			t.Logf("HTTP request from getter failed with '%s'", err)
		}

		buffer := bytes.NewBuffer(nil)
		buffer.Write([]byte(strconv.Itoa(*peerIndex) + ":" + key))
		return view.NewView(buffer), nil
	})
	_, err = NewMemory("httpPoolTest", 1<<20, getter, cache.Options{Logger: logrus.New()})
	if err != nil {
		panic(err)
	}

	log.Fatal(http.ListenAndServe(addrs[*peerIndex], p))
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	const max = 1 * time.Second
	tries := 0
	for {
		tries++
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		delay := time.Duration(tries) * 25 * time.Millisecond
		if delay > max {
			delay = max
		}
		time.Sleep(delay)
	}
}
