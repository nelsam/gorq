package extensions

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

const (
	DefaultSRID = 4326
)

// Geography maps against Postgis geographical point.
type Geography struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// String returns a string representation of p.
func (g *Geography) String() string {
	return fmt.Sprintf("GEOMETRY(POINT(%v,%v))::GEOGRAPHY", g.Lat, g.Lng)
}

// Scan implements "database/sql".Scanner and will scan the Postgis POINT(x y)
// into p.
func (g *Geography) Scan(val interface{}) error {
	b, err := hex.DecodeString(string(val.([]uint8)))
	if err != nil {
		return err
	}

	r := bytes.NewReader(b)
	var wkbByteOrder uint8
	if err := binary.Read(r, binary.LittleEndian, &wkbByteOrder); err != nil {
		return err
	}

	var byteOrder binary.ByteOrder
	switch wkbByteOrder {
	case 0:
		byteOrder = binary.BigEndian
	case 1:
		byteOrder = binary.LittleEndian
	default:
		return fmt.Errorf("invalid byte order %u", wkbByteOrder)
	}

	var wkbGeometryType uint64
	if err := binary.Read(r, byteOrder, &wkbGeometryType); err != nil {
		return err
	}

	if err := binary.Read(r, byteOrder, g); err != nil {
		return err
	}

	return nil
}

// Value implements "database/sql/driver".Valuer and will return the string
// representation of p by calling the String() method.
func (g *Geography) Value() (driver.Value, error) {
	return g.String(), nil
}

// TypeDef implements "github.com/outdoorsy/gorp".TypeDeffer and will return
// the type definition to be used when running a "CREATE TABLE" statement.
func (g *Geography) TypeDef() string {
	return fmt.Sprintf("GEOGRAPHY(POINT, %d)", DefaultSRID)
}
