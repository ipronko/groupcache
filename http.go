/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipronko/groupcache/consistenthash"
	"github.com/ipronko/groupcache/view"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

const (
	sizeHeader   = "X-Size"
	expireHeader = "X-Expire"
)

type ServiceDiscovery interface {
	Register(serviceAddr, httpHealthAddr string) error
	Watch(ctx context.Context, watchFunc func(addr ...string)) error
}

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	mu          sync.Mutex // guards peers and httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, uses the http.Request.Context()
	Context func(*http.Request) context.Context

	ServiceDiscovery ServiceDiscovery

	Auth string
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(ctx context.Context, self string) (*HTTPPool, error) {
	p, err := NewHTTPPoolOpts(ctx, self, nil)
	if err != nil {
		return nil, err
	}
	http.Handle(p.opts.BasePath, p)
	return p, nil
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(ctx context.Context, self string, o *HTTPPoolOptions) (*HTTPPool, error) {
	if httpPoolMade {
		return nil, fmt.Errorf("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	err := RegisterPeerPicker(func() PeerPicker { return p })
	if err != nil {
		return nil, err
	}

	if p.opts.ServiceDiscovery != nil {
		err := p.opts.ServiceDiscovery.Register(self, fmt.Sprintf("%s/%s", self, filepath.Join(p.opts.BasePath, "health")))
		if err != nil {
			return nil, err
		}
		go func() {
			err := p.opts.ServiceDiscovery.Watch(ctx, p.Set)
			if err != nil {
				logger.Errorf("watch to service updates err: %s", err.Error())
			}
		}()
	}
	return p, nil
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			getTransport: p.opts.Transport,
			baseURL:      peer + p.opts.BasePath,
			auth:         p.opts.Auth,
		}
	}
}

// GetAll returns all the peers in the pool
func (p *HTTPPool) GetAll() []ProtoGetter {
	p.mu.Lock()
	defer p.mu.Unlock()

	var i int
	res := make([]ProtoGetter, len(p.httpGetters))
	for _, v := range p.httpGetters {
		res[i] = v
		i++
	}
	return res
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 3)
	if len(parts) > 2 || len(parts) < 1 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if parts[len(parts)-1] == "health" {
		w.Write([]byte("ok"))
		return
	}

	if p.opts.Auth != "" {
		auth := r.Header.Get("Authorization")
		if auth != p.opts.Auth {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.opts.Context != nil {
		ctx = p.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	group.Stats.ServerRequests.Add(1)

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.localRemove(key)
		return
	}

	v, err := group.Get(ctx, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer v.Close()

	w.Header().Set(sizeHeader, fmt.Sprintf("%d", v.Len()))
	w.Header().Set(expireHeader, fmt.Sprintf("%d", v.Expire()))

	io.Copy(w, v)
}

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	baseURL      string
	auth         string
}

// GetURL
func (h *httpGetter) GetURL() string {
	return h.baseURL
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpGetter) makeRequest(ctx context.Context, method string, in *GetRequest, out *http.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.Group),
		url.QueryEscape(in.Key),
	)
	req, err := http.NewRequest(method, u, nil)
	if err != nil {
		return err
	}

	// Pass along the context to the RoundTripper
	req = req.WithContext(ctx)

	if h.auth != "" {
		req.Header.Add("Authorization", h.auth)
	}

	tr := http.DefaultTransport
	if h.getTransport != nil {
		tr = h.getTransport(ctx)
	}

	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	*out = *res
	return nil
}

func (h *httpGetter) Get(ctx context.Context, in *GetRequest) (*view.View, error) {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, &res); err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	sizeStr := res.Header.Get(sizeHeader)
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, err
	}

	durStr := res.Header.Get(expireHeader)
	duration, err := strconv.ParseInt(durStr, 10, 64)
	if err != nil {
		return nil, err
	}

	return view.NewView(res.Body, size, time.Duration(duration)), nil
}

func (h *httpGetter) Remove(ctx context.Context, in *GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}
