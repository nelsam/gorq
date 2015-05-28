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

func CacheKey(query string, args []interface{}) (string, error) {
	key := fmt.Sprintf("%s: %v", query, args)
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

type cacheMapping struct {
	cacheType       reflect.Type
	cacheableFields map[string]*cacheField
}

func (c *cacheMapping) add(nested []*fieldColumnMap) {
	targetField := nested[len(nested)-1]
	key := targetField.alias
	if c.has(key) {
		return
	}
	newField := &cacheField{}
	for _, f := range nested {
		newField.idx = append(newField.idx, f.column.FieldIndex()...)
		field := c.cacheType.FieldByIndex(newField.idx)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Slice && f != targetField {
			if field, ok := c.cacheableFields[f.column.JoinAlias()]; ok {
				// f is mapped to a slice type, and m is nested inside
				// of the slice's elements.  Let field handle it.
				field.add(nested)

				// This has already been added, so skip the map assignment
				return
			}
			// f is mapped to a slice type that has not yet been
			// mapped.
			subType := fieldType.Elem()
			if subType.Kind() == reflect.Ptr {
				subType = subType.Elem()
			}
			newField.elemFields = &cacheMapping{cacheType: subType}
			key = f.column.JoinAlias()
			newField.alias = key
			newField.add(nested)
			break
		}
	}
	c.set(key, newField)
}

func (c *cacheMapping) has(key string) bool {
	if c.cacheableFields == nil {
		return false
	}
	_, ok := c.cacheableFields[key]
	return ok
}

func (c *cacheMapping) set(key string, value *cacheField) {
	if c.cacheableFields == nil {
		c.cacheableFields = map[string]*cacheField{}
	}
	c.cacheableFields[key] = value
}

func (c *cacheMapping) valueFor(src reflect.Value) interface{} {
	for src.Kind() == reflect.Ptr || src.Kind() == reflect.Interface {
		if src.IsNil() {
			return nil
		}
		src = src.Elem()
	}
	if src.Kind() == reflect.Slice {
		res := make([]interface{}, 0, src.Len())
		for i := 0; i < src.Len(); i++ {
			res = append(res, c.valueFor(src.Index(i)))
		}
		return res
	}
	res := map[string]interface{}{}
	for name, mapping := range c.cacheableFields {
		field := fieldOrNilByIndex(src, mapping.idx)
		value := field.Interface()
		if mapping.elemFields != nil {
			value = mapping.elemFields.valueFor(field)
		}
		res[name] = value
	}
	return res
}

type cacheField struct {
	alias      string
	idx        []int
	elemFields *cacheMapping
}

func (c *cacheField) add(nested []*fieldColumnMap) {
	names := ""
	for _, n := range nested {
		names += n.column.ColumnName + "(" + n.column.JoinAlias() + "), "
	}
	for nested[0].column.JoinAlias() != c.alias {
		nested = nested[1:]
	}
	c.elemFields.add(nested[1:])
}
