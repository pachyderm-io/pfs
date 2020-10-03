package localcache

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
)

// Cache is a simple unbounded disk cache and is safe for concurrency.
type Cache struct {
	root string
	keys map[string]bool
	mu   sync.Mutex
}

// keyToFilename encodes a key to a filename to allow characters (such as '/') in keys.
func keyToFilename(key string) string {
	return url.PathEscape(key)
}

// NewCache creates a new cache.
func NewCache(root string) (*Cache, error) {
	if err := os.MkdirAll(root, 0777); err != nil {
		return nil, errors.EnsureStack(err)
	}

	return &Cache{
		root: root,
		keys: make(map[string]bool),
	}, nil
}

// Has returns true if the key is present in the cache, false otherwise.
func (c *Cache) Has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.keys[key]
	return ok
}

// Put puts a key/value pair in the cache and reads the value from an io.Reader.
func (c *Cache) Put(key string, value io.Reader) (retErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path := filepath.Join(c.root, keyToFilename(key))
	f, err := os.Create(path)
	if err != nil {
		return errors.EnsureStack(err)
	}
	defer func() {
		if err := f.Close(); err != nil && retErr == nil {
			retErr = errors.EnsureStack(err)
		}
		if retErr != nil {
			os.Remove(path)
			delete(c.keys, key)
		}
	}()
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	if _, err := io.CopyBuffer(f, value, buf); err != nil {
		return errors.EnsureStack(err)
	}
	c.keys[key] = true
	return nil
}

// Get gets a key's value by returning an io.ReadCloser that should be closed when done.
func (c *Cache) Get(key string) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.keys[key] {
		return nil, errors.Errorf("key %v not found in cache", key)
	}
	f, err := os.Open(filepath.Join(c.root, keyToFilename(key)))
	if err != nil {
		return nil, errors.EnsureStack(err)
	}
	return f, nil
}

// Keys returns the keys in sorted order.
func (c *Cache) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for key := range c.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Delete deletes a key/value pair.
func (c *Cache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.keys[key] {
		return nil
	}
	delete(c.keys, key)
	return errors.EnsureStack(os.Remove(filepath.Join(c.root, keyToFilename(key))))
}

// Clear clears the cache.
func (c *Cache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() {
		c.keys = make(map[string]bool)
	}()
	for key := range c.keys {
		if err := os.Remove(filepath.Join(c.root, keyToFilename(key))); err != nil {
			return errors.EnsureStack(err)
		}
	}
	return nil
}

// Close clears the cache and removes the directory associated with the cache
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = make(map[string]bool)
	return os.RemoveAll(c.root)
}
