package cache

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/oxtoacart/bpool"

	"github.com/ipronko/groupcache/popular"
	"github.com/ipronko/groupcache/view"
)

type FileOptions struct {
	Options
	SkipFirstCalls *int
	RootPath       string
}

var (
	defaultSkipFirstCalls = 1
)

func (o *FileOptions) Complete(maxSize int64) {
	o.Options.Complete(maxSize)

	if o.RootPath == "" {
		o.RootPath = os.TempDir()
	}
	if o.SkipFirstCalls == nil {
		o.SkipFirstCalls = &defaultSkipFirstCalls
	}
}

func NewFile(maxSize int64, opts FileOptions) (*file, error) {
	opts.Complete(maxSize)

	rCache, err := getCache(maxSize, opts.Options)
	if err != nil {
		return nil, err
	}

	c := &file{
		maxInstanceSize: opts.MaxInstanceSize,
		cache:           rCache,
		bufPool:         bpool.NewBytePool(opts.CopyBufferSize, opts.CopyBufferWidth),
		logger:          opts.Logger,
		popularFiles:    popular.New(*opts.SkipFirstCalls, time.Hour*24*30),
	}

	fr, err := newFileResolver(opts.RootPath)
	if err != nil {
		return nil, err
	}
	c.fileResolver = fr

	go c.restoreFiles()

	return c, nil
}

func onEvict(value interface{}, logger Logger) {
	val, ok := value.(fileValue)
	if !ok {
		return
	}
	err := val.delete()
	if err != nil {
		logger.Errorf("delete file %s err: %s", val.filePath, err)
	}
}

// file is a wrapper around an *ristretto.Cache
type file struct {
	bufPool         *bpool.BytePool
	maxInstanceSize int64
	logger          Logger
	cache           *ristretto.Cache

	fileResolver *fileResolver
	popularFiles *popular.HitStore
}

func (c *file) Stats() CacheStats {
	return CacheStats{
		Bytes:     c.cache.Metrics.CostAdded() - c.cache.Metrics.CostEvicted(),
		Items:     c.cache.Metrics.KeysAdded() - c.cache.Metrics.KeysEvicted(),
		Gets:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Hits:      c.cache.Metrics.GetsKept() + c.cache.Metrics.GetsDropped(),
		Evictions: c.cache.Metrics.KeysEvicted(),
	}
}

func (c *file) restoreFiles() {
	fileCh := make(chan fileValue)
	go func() {
		err := c.fileResolver.walk(fileCh)
		if err != nil {
			c.logger.Errorf("add existing files to memoryCache: file walk err: %s", err.Error())
		}
	}()

	for file := range fileCh {
		c.cache.Set(filepath.Base(file.filePath), file, file.size)
	}
}

func (c *file) Add(key string, value *view.View) error {
	if value.Len() > c.maxInstanceSize {
		return nil
	}

	if !c.popularFiles.IsPopular(key) {
		return nil
	}

	return c.set(key, value)
}

func (c *file) AddForce(key string, value *view.View) error {
	defer value.Close()

	if value.Len() > c.maxInstanceSize {
		return nil
	}

	return c.readAndSet(key, value, value.Len(), value.Expire(), true)
}

func (c *file) set(key string, value *view.View) error {
	//What if value is buffer?
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

		err := c.readAndSet(key, teeReader, value.Len(), value.Expire(), false)
		if err != nil {
			c.logger.Errorf("key: %s, readAndSet value err: %s", err.Error())
		}
	}()

	return nil
}

func (c *file) readAndSet(key string, r io.Reader, len int64, expire time.Duration, force bool) error {
	bullPool := c.bufPool.Get()
	defer c.bufPool.Put(bullPool)

	tmpFile, err := c.fileResolver.tmpFile(key)
	if err != nil {
		return fmt.Errorf("create temp file err: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	wrote, err := io.CopyBuffer(tmpFile, r, bullPool)
	if err != nil {
		return fmt.Errorf("copy from reader to bytes buffer err: %w", err)
	}

	file := c.fileResolver.newFile(key, len, expire)

	err = c.fileResolver.moveToFiles(tmpFile.Name(), file.filePath)
	if err != nil {
		return fmt.Errorf("move tmp file to files err: %w", err)
	}

	ok := c.setValue(key, file, wrote, expire, force)
	if !ok {
		err := c.fileResolver.delete(key)
		if err != nil {
			return fmt.Errorf("copy from reader to bytes buffer err: %w", err)
		}
	}

	return nil
}

func (c *file) setValue(key string, val fileValue, len int64, expire time.Duration, force bool) bool {
	for i := 0; i < 1000; i++ {
		if c.cache.SetWithTTL(key, val, len, expire) {
			return true
		}
		if !force {
			break
		}
	}
	return false
}

func (c *file) Get(key string) (v *view.View, ok bool) {
	vi, ok := c.cache.Get(key)
	if !ok {
		return
	}

	f, ok := vi.(fileValue)
	if !ok {
		c.Remove(key)
	}

	v, err := f.readerView()
	if err != nil {
		return nil, false
	}

	return v, ok
}

func (c *file) Remove(key string) {
	c.cache.Del(key)
	err := c.fileResolver.delete(key)
	if err != nil {
		c.logger.Errorf("delete %s key err: %s", key, err)
	}
}

const (
	tempPath = "tmp"
	filePath = "file"
)

func newFileResolver(rootDir string) (*fileResolver, error) {
	tmpRoot := filepath.Join(rootDir, tempPath)
	fileRoot := filepath.Join(rootDir, filePath)

	if err := os.MkdirAll(tmpRoot, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create dirs for tmp files")
	}
	if err := os.MkdirAll(fileRoot, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create dirs for files")
	}

	return &fileResolver{
		tmpRoot:  tmpRoot,
		fileRoot: fileRoot,
	}, nil
}

type fileResolver struct {
	tmpRoot  string
	fileRoot string
}

func (f *fileResolver) walk(fileCh chan<- fileValue) error {
	defer func() { close(fileCh) }()

	err := filepath.Walk(f.fileRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		fileCh <- fileValue{
			filePath: path,
			size:     info.Size(),
			ttl:      0,
		}
		return nil
	})

	return err
}

func (f *fileResolver) delete(key string) error {
	if err := os.Remove(filepath.Join(getFilePath(key), key)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (f *fileResolver) newFile(key string, size int64, ttl time.Duration) fileValue {
	return fileValue{
		filePath: filepath.Join(f.fileRoot, getFilePath(key)),
		size:     size,
		ttl:      ttl,
	}
}

func (f *fileResolver) tmpFile(key string) (*os.File, error) {
	return ioutil.TempFile(f.tmpRoot, key)
}

func (f *fileResolver) moveToFiles(from, to string) error {
	err := os.MkdirAll(filepath.Dir(to), 0700)
	if err != nil {
		return fmt.Errorf("creating dirs for file %s err: %w", to, err)
	}

	err = os.Rename(from, to)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("move file from temp to file dir err: %s", err.Error())
	}
	return nil
}

type fileValue struct {
	filePath string
	size     int64
	ttl      time.Duration
}

func (f fileValue) readerView() (*view.View, error) {
	open, err := os.Open(f.filePath)
	if err != nil {
		return nil, fmt.Errorf("open %s file err: %w", f.filePath, err)
	}
	return view.NewView(open, f.size, f.ttl), nil
}

func (f fileValue) delete() error {
	if err := os.Remove(f.filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func getFilePath(key string) string {
	hash := sha1.New()
	hash.Write([]byte(key))
	sum := hash.Sum(nil)
	keySha := hex.EncodeToString(sum)

	return filepath.Join(keySha[:2], keySha[2:4], key)
}
