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

func restoreFromCache(encoded string, targetType reflect.Value, table reflect.Type) ([]interface{}, error) {
	r, err := gzip.NewReader(strings.NewReader(encoded))
	if err != nil {
		r.Close()
		return nil, err
	}

	typed := reflect.Zero(reflect.SliceOf(targetType.Type())).Interface()
	err = json.NewDecoder(r).Decode(&typed)
	if err != nil {
		return nil, err
	}
	err = r.Close()
	if err != nil {
		return nil, err
	}

	tv := reflect.ValueOf(typed)
	data := []interface{}{}
	for i := 0; i < tv.Len(); i++ {
		if targetType.Type().Kind() == reflect.Map {
			data = append(data, tv.Index(i).Interface())
		} else {
			data = append(data, decodeFromMemcache(tv.Index(i).Interface(), targetType, table))
		}
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

func getCacheData(cacheKey string, targetType reflect.Value, table reflect.Type, cache *mc.Conn) ([]interface{}, error) {
	s, _, _, err := cache.Get(cacheKey)
	if err != nil {
		return nil, err
	}
	return restoreFromCache(s, targetType, table)
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

func decodeFromMemcache(data interface{}, targetType reflect.Value, table reflect.Type) interface{} {
	dv := reflect.ValueOf(data)
	switch dv.Kind() {
	case reflect.Slice, reflect.Array:
		res := reflect.MakeSlice(dv.Type(), dv.Len(), dv.Cap())
		for i := 0; i < dv.Len(); i++ {
			decoded := decodeFromMemcache(dv.Index(i).Interface(), dv.Index(i), dv.Index(i).Type())
			res = reflect.Append(res, reflect.ValueOf(decoded))
		}
		return res
	case reflect.Struct:
		var elem reflect.Value
		if targetType.Kind() == reflect.Ptr {
			elem = reflect.Zero(targetType.Type())
		} else {
			elem = reflect.New(targetType.Type().Elem())
		}
	case reflect.Map:
		res := reflect.MakeMap(dv.Type())
		for _, key := range dv.MapKeys() {
			val := dv.MapIndex(key)
			decoded := decodeFromMemcache(val.Interface(), val, val.Type())
			val.SetMapIndex(key, reflect.ValueOf(decoded))
		}
		return res
	}

	return data
}
