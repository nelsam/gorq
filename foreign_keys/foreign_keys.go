// Package foreign_keys includes logic for helping with foreign key
// relationships.  Most of the time, foreign keys can be handled just
// fine using models as fields within other models; however, sometimes
// it just doesn't make sense to keep all models with foreign key
// relationships in the same package.
//
// Go has a very good method of handling situations like that:
// interfaces.  The issue with that is that when the interface field
// is nil and you send your model off to database libraries, the
// database libraries don't know how to initialize it, and end up
// spitting out errors.
//
// This package helps out with that issue by providing a way to
// register foreign keys for interface fields.  An example:
//
// ```
// func init() {
//     gorp.MapTableWithName(User{}, "messages").SetKeys(true, "Id")
//     foreign_keys.Register(communications.Message{}, "User", new(User))
// }
// ```
package foreign_keys

import (
	"errors"
	"fmt"
	"reflect"
)

var relationships []*relationship

func pathFor(t reflect.Type) string {
	return fmt.Sprintf(`"%s".%s`, t.PkgPath(), t.Name())
}

type relationship struct {
	parentTypePath string
	fieldName      string
	targetType     reflect.Type
}

func Register(parent, fieldPtrOrName interface{}, fkey Keyer) (registerErr error) {
	relation := new(relationship)
	relation.parentTypePath, relation.fieldName, registerErr = namesFor(parent, fieldPtrOrName)
	if registerErr != nil {
		return
	}
	if _, err := Lookup(parent, fieldPtrOrName); err == nil {
		message := fmt.Sprintf("There is already a registered foreign key type for type %s, field %s",
			relation.parentTypePath, relation.fieldName)
		return errors.New(message)
	}
	relation.targetType = reflect.TypeOf(fkey)
	relationships = append(relationships, relation)
	return
}

func namesFor(parent, fieldPtrOrName interface{}) (parentPath, fieldName string, err error) {
	parentVal := reflect.ValueOf(parent)
	parentType := parentVal.Type()
	if parentType.Kind() == reflect.Ptr {
		parentType = parentType.Elem()
		if parentVal.IsNil() {
			parentVal.Set(reflect.New(parentType))
		}
		parentVal = parentVal.Elem()
	}

	parentPath = pathFor(parentType)

	var ok bool
	if fieldName, ok = fieldPtrOrName.(string); ok {
		return
	}

	for i := 0; i < parentVal.NumField(); i++ {
		field := parentVal.Field(i)
		fieldType := parentType.Field(i)
		if fieldType.Anonymous {
			if _, fieldName, err = namesFor(field.Addr().Interface(), fieldPtrOrName); err == nil {
				return
			}
		} else {
			if fieldType.PkgPath == "" && field.Addr().Interface() == fieldPtrOrName {
				fieldName = fieldType.Name
				return
			}
		}
	}
	return "", "", errors.New("The requested field pointer was not found on the parent value")
}

func Lookup(parent, fieldPtrOrName interface{}) (Keyer, error) {
	parentPath, fieldName, err := namesFor(parent, fieldPtrOrName)
	if err != nil {
		return nil, err
	}
	for _, relation := range relationships {
		if relation.parentTypePath == parentPath && relation.fieldName == fieldName {
			targetType := relation.targetType
			var targetVal reflect.Value
			if targetType.Kind() == reflect.Ptr {
				targetVal = reflect.New(targetType.Elem())
			} else {
				targetVal = reflect.Zero(targetType)
			}
			return targetVal.Interface().(Keyer), nil
		}
	}
	message := fmt.Sprintf("Could not find a registered foreign key relationship for type %s, field %s",
		parentPath, fieldName)
	return nil, errors.New(message)
}
