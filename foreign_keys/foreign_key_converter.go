package foreign_keys

import (
	"reflect"

	"github.com/nelsam/gorp"
)

// ForeignKeyConverter is a gorp.TypeConverter that converts
// foreign keys.
type ForeignKeyConverter struct {
	SubConverter gorp.TypeConverter
}

// ToDb will attempt to typecast the passed in value to Keyer and
// return the value's key.
func (converter ForeignKeyConverter) ToDb(parent, value interface{}) (dbValue interface{}, err error) {
	if converter.SubConverter != nil {
		defer func() {
			if err == nil {
				dbValue, err = converter.SubConverter.ToDb(dbValue)
			}
		}()
	}
	// Use reflect to check if value is nil.  This works around
	// situations where value's type is not nil, but its value is.
	refValue := reflect.ValueOf(value)
	if (refValue.Kind() == reflect.Ptr || refValue.Kind() == reflect.Interface) && refValue.IsNil() {
		return nil, nil
	}
	if keyer, ok := value.(Keyer); ok {
		return keyer.Key(), nil
	}
	return value, nil
}

// FromDb takes a pointer to a field within a struct.  If the field's
// Kind is reflect.Interface and its value is nil, FromDb will attempt
// to initialize it using the registered foreign key relationships.
func (converter ForeignKeyConverter) FromDb(parent, target interface{}) (scanner gorp.CustomScanner, convert bool) {
	if converter.SubConverter != nil {
		defer func() {
			if convert == false {
				scanner, convert = converter.SubConverter.FromDb(target)
			}
		}()
	}

	targetVal := reflect.ValueOf(target).Elem()
	if targetVal.Kind() == reflect.Interface && targetVal.IsNil() {
		if init, err := Lookup(parent, target); err == nil {
			targetVal.Set(reflect.ValueOf(init))
			convert = true

			scanner.Target = init
			holderPtrVal := reflect.New(reflect.TypeOf(init.Key()))
			scanner.Holder = holderPtrVal.Interface()
			scanner.Binder = func(interface{}, interface{}) error {
				if holderPtrVal.IsNil() {
					targetVal.Set(reflect.ValueOf(nil))
					return nil
				}
				holderVal := holderPtrVal.Elem()
				return init.SetKey(holderVal.Interface())
			}
		}
	}

	return
}
