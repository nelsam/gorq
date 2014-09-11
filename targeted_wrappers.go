package gorp_queries

import (
	"github.com/nelsam/gorp_queries/foreign_keys"
)

func (m *DbMap) cloneWithFkeyConverter(parent interface{}) *DbMap {
	newMap := &(*m)
	newMap.TypeConverter = &foreign_keys.ForeignKeyConverter{
		Parent:       parent,
		SubConverter: m.TypeConverter,
	}
	return newMap
}

func (m *DbMap) realGet(i interface{}, keys ...interface{}) (interface{}, error) {
	return m.DbMap.Get(i, keys...)
}

func (m *DbMap) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return m.cloneWithFkeyConverter(i).realGet(i, keys...)
}

func (m *DbMap) realSelect(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return m.DbMap.Select(i, query, args...)
}

func (m *DbMap) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return m.cloneWithFkeyConverter(i).realSelect(i, query, args...)
}

func (t *Transaction) cloneWithFkeyConverter(parent interface{}) *Transaction {
	newTrans := &(*t)
	newTrans.dbmap = newTrans.dbmap.cloneWithFkeyConverter(parent)
	return newTrans
}

func (t *Transaction) realGet(i interface{}, keys ...interface{}) (interface{}, error) {
	return t.Transaction.Get(i, keys...)
}

func (t *Transaction) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return t.cloneWithFkeyConverter(i).realGet(i, keys...)
}

func (t *Transaction) realSelect(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return t.Transaction.Select(i, query, args...)
}

func (t *Transaction) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return t.cloneWithFkeyConverter(i).realSelect(i, query, args...)
}
