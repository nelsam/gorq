package plans

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/memcachier/mc"
	"github.com/outdoorsy/gorp"
)

var (
	tableCacheMap  = map[string]map[string]struct{}{}
	tableCacheLock sync.RWMutex
)

func addTableCacheMapEntry(tableKey, cacheKey string) {
	tableCacheLock.Lock()
	if tableCacheMap[tableKey] == nil {
		tableCacheMap[tableKey] = map[string]struct{}{cacheKey: {}}
	} else {
		tableCacheMap[tableKey][cacheKey] = struct{}{}
	}
	tableCacheLock.Unlock()
}
func getTableCacheMapEntry(tableKey string) []string {
	var cacheKeys map[string]struct{}
	tableCacheLock.RLock()
	cacheKeys = tableCacheMap[tableKey]
	tableCacheLock.RUnlock()

	entries := make([]string, 0, len(cacheKeys))
	for k := range cacheKeys {
		entries = append(entries, k)
	}

	return entries
}

const defaultCacheExpirationTime = 604800 // one week

func prepareForCache(data interface{}, colMap structColumnMap) (string, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	err := json.NewEncoder(w).Encode(encodeForMemcache(data, colMap))
	if err != nil {
		w.Close()
		return "", err
	}
	err = w.Flush()
	if err != nil {
		w.Close()
		return "", err
	}
	err = w.Close()
	if err != nil {
		return "", err
	}
	return string(b.Bytes()), nil
}

func restoreFromCache(encoded string, target reflect.Value, table *gorp.TableMap) ([]interface{}, error) {
	r, err := gzip.NewReader(strings.NewReader(encoded))
	if err != nil {
		r.Close()
		return nil, err
	}

	var data interface{}
	err = json.NewDecoder(r).Decode(&data)
	if err != nil {
		return nil, err
	}
	err = r.Close()
	if err != nil {
		return nil, err
	}

	var elem reflect.Value
	if target.Kind() == reflect.Ptr {
		elem = reflect.New(target.Type().Elem())
	} else {
		elem = reflect.Zero(target.Type())
	}
	data = decodeFromMemcache(data, elem, table)

	switch src := data.(type) {
	case interface{}:
		return []interface{}{src}, nil
	case []interface{}:
		return src, nil
	}
	return nil, errors.New("return type may only be []interface{}. it was: " + reflect.TypeOf(data).Name())
}

func setCacheData(cacheKey string, data interface{}, colMap structColumnMap, cache *mc.Conn) error {
	encoded, err := prepareForCache(data, colMap)
	if err != nil {
		return err
	}
	_, err = cache.Set(cacheKey, encoded, 0, defaultCacheExpirationTime, 0)
	if err == nil {
		fmt.Println("SET CACHE - ", cacheKey)
	} else {
		fmt.Println("SET CACHE FAILED!! - ", cacheKey, err)
	}
	return err
}

func getCacheData(cacheKey string, target reflect.Value, table *gorp.TableMap, cache *mc.Conn) ([]interface{}, error) {
	s, _, _, err := cache.Get(cacheKey)
	if err != nil {
		return nil, err
	}
	data, err := restoreFromCache(s, target, table)
	if err == nil {
		fmt.Println("READ CACHE - ", cacheKey)
	} else {
		fmt.Println("READ CACHE FAILED - ", cacheKey, err)

	}

	return data, nil
}

func evictCacheData(cacheKeys []string, cache *mc.Conn) error {
	for _, key := range cacheKeys {
		err := cache.Del(key)
		if err != nil {
			fmt.Println("DEL CACHE FAILED - ", key, err)
			return err
		} else {
			fmt.Println("DEL CACHE", key)
		}
	}
	return nil
}

func encodeForMemcache(data interface{}, colMap structColumnMap) interface{} {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		// indirect the pointer to handle it transparently
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Slice, reflect.Array:
		res := make([]interface{}, 0, val.Len())
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			res = append(res, encodeForMemcache(elem.Interface(), colMap))
		}
		return res
	case reflect.Struct:
		res := make(map[string]interface{})
		for _, m := range colMap {
			key := m.alias
			if key == "-" {
				continue
			}
			index := m.column.FieldIndex()
			res[key] = fieldByIndex(val, index)
		}
		return res
	case reflect.Map:
		res := make(map[string]interface{})
		keys := val.MapKeys()
		for _, key := range keys {
			res[key.String()] = encodeForMemcache(val.MapIndex(key), colMap)
		}
	}
	return data
}

func decodeFromMemcache(from interface{}, to reflect.Value, table *gorp.TableMap) interface{} {
	switch src := from.(type) {
	case []interface{}:
		for _, v := range src {
			reflect.Append(to, reflect.ValueOf(decodeFromMemcache(v, to, table)))
		}
		return to.Interface()
	case map[string]interface{}:
		for k, v := range src {
			col := table.ColMap(k)
			if col == nil {
				// return an error, probably, since the target type does not have a field to apply this value to
			}
			fieldByIndex(to, col.FieldIndex()).Set(reflect.ValueOf(v))
		}
		return to.Interface()
	default:
		// bool, float64, string, or nil
		return to.Interface()
	}
}

func generateCacheKey(query string, plan *QueryPlan) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s: %v", query, plan.args)))
	cacheKey := base64.StdEncoding.EncodeToString(digest[:])
	return cacheKey
}

func getTableForCache(plan *QueryPlan) (*gorp.TableMap, error) {
	e := plan.target
	if e.Kind() == reflect.Ptr {
		e = e.Elem()
	}
	return plan.dbMap.TableFor(e.Type(), false)
}
