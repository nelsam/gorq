package plans

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/memcachier/mc"
)

const defaultCacheExpirationTime = 0 // never expire

func prepareForCache(data interface{}) (string, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	err := json.NewEncoder(w).Encode(convertToMemcacheVal(data))
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

func restoreFromCache(encoded string) ([]interface{}, error) {
	r, err := gzip.NewReader(strings.NewReader(encoded))
	if err != nil {
		r.Close()
		return nil, err
	}
	var data []interface{}
	err = json.NewDecoder(r).Decode(&data)
	if err != nil {
		return nil, err
	}
	err = r.Close()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func setCacheData(cacheKey string, data interface{}, cache *mc.Conn) error {
	encoded, err := prepareForCache(data)
	if err != nil {
		return err
	}
	_, err = cache.Set(cacheKey, encoded, 0, defaultCacheExpirationTime, 0)
	return err
}

func getCacheData(cacheKey string, cache *mc.Conn) ([]interface{}, error) {
	s, _, _, err := cache.Get(cacheKey)
	if err != nil {
		return nil, err
	}
	return restoreFromCache(s)
}

func convertToMemcacheVal(data interface{}) interface{} {
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
			res = append(res, convertToMemcacheVal(elem.Interface()))
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
			res[name] = convertToMemcacheVal(field.Interface())
		}
		return res
	case reflect.Map:
		res := make(map[string]interface{})
		keys := val.MapKeys()
		for _, key := range keys {
			res[key.String()] = convertToMemcacheVal(val.MapIndex(key))
		}
	}
	return data
}
