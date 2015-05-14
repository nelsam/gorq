package plans

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strings"

	"github.com/memcachier/mc"
)

const defaultCacheExpirationTime = 0 // never expire

// TODO: use custom map encoder instead of json.Marshal
// Use column names to generate the map

func prepareForCache(data interface{}) (string, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	err := json.NewEncoder(w).Encode(data)
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
