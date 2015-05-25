package plans

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func (plan *QueryPlan) CacheKey() (string, error) {
	query, err := plan.selectQuery()
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s: %v", query, plan.args)
	digest := sha256.Sum256([]byte(key))
	return base64.StdEncoding.EncodeToString(digest[:]), nil
}

func fullTypePath(object interface{}) string {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + "." + t.Name()
}

func prepareForCache(data string) (encoded string, err error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	defer func() {
		closeErr := w.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	err = json.NewEncoder(w).Encode(data)
	if err != nil {
		return "", err
	}
	err = w.Flush()
	if err != nil {
		return "", err
	}
	return string(b.Bytes()), nil
}

func restoreFromCache(encoded string) (decoded []interface{}, err error) {
	r, err := gzip.NewReader(strings.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := r.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	err = json.NewDecoder(r).Decode(&decoded)
	if err != nil {
		return nil, err
	}

	return
}
