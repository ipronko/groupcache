package cache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/djherbis/fscache"
	"github.com/oxtoacart/bpool"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

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

	c := &file{
		maxInstanceSize: opts.MaxInstanceSize,
		logger:          opts.Logger,
		popularFiles:    popular.New(*opts.SkipFirstCalls, time.Hour*24*30),
	}

	fr, err := newFileResolver(opts.RootPath, opts.CopyBufferSize, opts.CopyBufferWidth)
	if err != nil {
		return nil, err
	}
	c.fileResolver = fr

	rCache, err := getCache(maxSize, opts.Options, func(value interface{}) {
		err := fr.evict(value)
		if err != nil {
			opts.Logger.Errorf("evict file err: %s", err.Error())
		}
	})
	if err != nil {
		return nil, err
	}

	c.cache = rCache

	go c.restoreFiles()

	return c, nil
}

// file is a wrapper around an *ristretto.Cache
type file struct {
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

	var filesAdded int64
	for file := range fileCh {
		ok := c.cache.Set(filepath.Base(file.filePath), file, file.size)
		if !ok {
			err := file.delete()
			if err != nil {
				c.logger.Errorf("delete file %s error: %s", file.filePath, err.Error())
			}
			continue
		}
		filesAdded++
	}
	c.logger.Infof("file cache: restore job: cache files restored: %d", filesAdded)
}

func (c *file) Add(key string, value *view.View) error {
	if !c.popularFiles.IsPopular(key) {
		return nil
	}

	return c.set(key, value, false)
}

func (c *file) AddForce(key string, value *view.View) error {
	return c.set(key, value, true)
}

func (c *file) set(key string, value *view.View, force bool) error {
	reader, writer, err := c.fileResolver.createTemp(key)
	if err != nil {
		c.logger.Errorf("skip creating tmp file, err: %s", err.Error())
		return nil
	}

	oldReader := value.SwapReader(reader)
	if writer == nil {
		if rc, ok := oldReader.(io.ReadCloser); ok {
			rc.Close()
		}
		return nil
	}

	go func() {
		defer func() {
			if rc, ok := oldReader.(io.ReadCloser); ok {
				rc.Close()
			}
		}()

		file, err := c.fileResolver.overTemp(key, oldReader, writer)
		if err != nil {
			c.fileResolver.delete(key)
			if !errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, io.ErrClosedPipe) {
				c.logger.Errorf("write to tmp file err: %s", err.Error())
			}
			return
		}

		ok := c.setValue(key, file, file.size, force)
		if !ok {
			err := c.fileResolver.delete(key)
			if err != nil {
				c.logger.Errorf("delete %s file err: %s", key, err.Error())
				return
			}
		}
	}()

	return nil
}

func (c *file) setValue(key string, val fileValue, len int64, force bool) bool {
	for i := 0; i < 1000; i++ {
		if c.cache.Set(key, val, len) {
			return true
		}
		if !force {
			break
		}
	}
	return false
}

func (c *file) Get(key string) (*view.View, bool) {
	vi, ok := c.cache.Get(key)
	if !ok {
		rc, ok := c.fileResolver.exists(key)
		if ok {
			return view.NewView(rc), ok
		}
		return nil, ok
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

func newFileResolver(rootDir string, copyBufferSize, copyBufferWidth int) (*fileResolver, error) {
	tmpRoot := filepath.Join(rootDir, tempPath)
	fileRoot := filepath.Join(rootDir, filePath)

	if err := os.RemoveAll(tmpRoot); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove previous tmp files err: %w", err)
	}

	if err := os.MkdirAll(tmpRoot, 0700); err != nil {
		return nil, fmt.Errorf("create dirs for tmp files")
	}

	if err := os.MkdirAll(fileRoot, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create dirs for files")
	}

	tmpCache, err := fscache.New(tmpRoot, 0600, 0)
	if err != nil {
		return nil, fmt.Errorf("create tmp fscache err: %w", err)
	}

	return &fileResolver{
		buffPool: bpool.NewBytePool(copyBufferSize, copyBufferWidth),
		tmpCache: tmpCache,
		fileRoot: fileRoot,
	}, nil
}

type fileResolver struct {
	tmpCache fscache.Cache
	fileRoot string
	buffPool *bpool.BytePool
}

func (f *fileResolver) exists(key string) (io.ReadCloser, bool) {
	rc, writer, err := f.tmpCache.Get(key)
	if err != nil {
		return nil, false
	}

	if writer != nil {
		//TODO log errors
		writer.Close()
		rc.Close()
		f.tmpCache.Remove(key)
		return nil, false
	}

	return rc, true
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

func (f *fileResolver) evict(value interface{}) error {
	val, ok := value.(fileValue)
	if !ok {
		return errors.Errorf("try evict not file value %T, %v", value, value)
	}
	err := val.delete()
	return errors.WithMessagef(err, "delete file %s", val.filePath)
}

func (f *fileResolver) newFile(key string, size int64) fileValue {
	return fileValue{
		filePath: filepath.Join(f.fileRoot, getFilePath(key)),
		size:     size,
	}
}

func (f *fileResolver) createTemp(key string) (fscache.ReadAtCloser, io.WriteCloser, error) {
	return f.tmpCache.Get(key)
}

func (f *fileResolver) overTemp(key string, r io.Reader, w io.WriteCloser) (fileV fileValue, err error) {
	bullPool := f.buffPool.Get()
	defer func() {
		f.buffPool.Put(bullPool)
		err := multierr.Append(err, errors.WithMessagef(w.Close(), "close tmp file writer"))
		err = multierr.Append(err, errors.WithMessagef(f.tmpCache.Remove(key), "remove tmp file key: %s", key))
	}()

	wrote, err := io.CopyBuffer(w, r, bullPool)
	if err != nil {
		return fileV, fmt.Errorf("copy from reader to bytes buffer err: %w", err)
	}

	fileV = f.newFile(key, wrote)

	nameGetter, ok := w.(interface{ Name() string })
	if !ok {
		return fileV, fmt.Errorf("cant resolve tmp file name, key: %s", key)
	}

	err = f.moveToFiles(nameGetter.Name(), fileV.filePath)
	if err != nil {
		return fileV, fmt.Errorf("move tmp file to files err: %w", err)
	}

	return fileV, nil
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
}

func (f fileValue) readerView() (*view.View, error) {
	open, err := os.Open(f.filePath)
	if err != nil {
		return nil, fmt.Errorf("open %s file err: %w", f.filePath, err)
	}
	return view.NewView(open), nil
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
