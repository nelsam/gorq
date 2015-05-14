package plans

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/memcachier/mc"
	"github.com/outdoorsy/gorp"
)

const defaultCacheExpirationTime = 0 // never expire

func prepareForCache(data interface{}) (string, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	err := json.NewEncoder(w).Encode(encodeForMemcache(data))
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
		elem = reflect.Zero(target.Type())
	} else {
		elem = reflect.New(target.Type().Elem())
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

func setCacheData(cacheKey string, data interface{}, cache *mc.Conn) error {
	encoded, err := prepareForCache(data)
	if err != nil {
		return err
	}
	_, err = cache.Set(cacheKey, encoded, 0, defaultCacheExpirationTime, 0)
	return err
}

func getCacheData(cacheKey string, target reflect.Value, table *gorp.TableMap, cache *mc.Conn) ([]interface{}, error) {
	s, _, _, err := cache.Get(cacheKey)
	if err != nil {
		return nil, err
	}
	return restoreFromCache(s, target, table)
}

func evictCacheData(cacheKeys []string, cache *mc.Conn) error {
	for _, key := range cacheKeys {
		err := cache.Del(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func encodeForMemcache(data interface{}) interface{} {
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
			res = append(res, encodeForMemcache(elem.Interface()))
		}
		return res
	case reflect.Struct:
		res := make(map[string]interface{})
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			fieldInfo := val.Type().Field(i)
			name := fieldInfo.Tag.Get("db")
			if idx := strings.IndexRune(name, ','); idx >= 0 {
				name = name[:idx]
			}
			res[name] = encodeForMemcache(field.Interface())
		}
		return res
	case reflect.Map:
		res := make(map[string]interface{})
		keys := val.MapKeys()
		for _, key := range keys {
			res[key.String()] = encodeForMemcache(val.MapIndex(key))
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
			to.FieldByIndex(col.FieldIndex()).Set(reflect.ValueOf(v))
		}
		return to.Interface()
	default:
		// bool, float64, string, or nil
		return to.Interface()
	}
}
