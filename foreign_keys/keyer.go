package foreign_keys

// A Keyer is a type which can return its key for foreign key
// relationships.
type Keyer interface {
	Key() interface{}
	SetKey(interface{}) error
}
