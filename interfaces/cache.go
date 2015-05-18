package interfaces

import "github.com/outdoorsy/gorp"

// Cache is a type used to handle caching data, via memcache or NoSQL
// stores.
type Cache interface {
	// Relate relates source to target, so that whenever keys for
	// source are dropped, keys for target are dropped as well.
	Relate(source, target *gorp.TableMap)

	// Set sets a key:value pair in the cache, and relates that key to
	// all entries in the tables slice.
	Set(tables []*gorp.TableMap, key, value string) error

	// Get returns the value for key.
	Get(key string) (string, error)

	// DropEntries should drop all entries in the cache related to
	// tables.
	DropEntries([]*gorp.TableMap) error

	// Cacheable returns whether or not table is cacheable.
	Cacheable(*gorp.TableMap) bool

	// SetCacheable sets whether or not table is cacheable.
	SetCacheable(*gorp.TableMap) bool
}
