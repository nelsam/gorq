package extensions

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/nelsam/gorq/filters"
)

const (
	// DefaultSRID represents the default SRID to use for PostGIS
	// geometry points representing a geographical location.
	DefaultSRID = 4326
)

// Geography maps against Postgis geographical point.
type Geography struct {
	Lng float64 `json:"lng"`
	Lat float64 `json:"lat"`
}

// String returns a string representation of p.
func (g Geography) String() string {
	return fmt.Sprintf("POINT(%v %v)", g.Lng, g.Lat)
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
func (g Geography) Value() (driver.Value, error) {
	return g.String(), nil
}

// TypeDef implements "github.com/outdoorsy/gorp".TypeDeffer and will return
// the type definition to be used when running a "CREATE TABLE" statement.
func (g Geography) TypeDef() string {
	return fmt.Sprintf("GEOGRAPHY(POINT, %d)", DefaultSRID)
}

// Polygon maps against Postgis geographical point.
type Polygon struct {
	Points []Geography `json:"points"`
}

// String returns a string representation of p.
func (p Polygon) String() string {
	b := bytes.NewBufferString("POLYGON((")
	for _, v := range p.Points {
		b.WriteString(fmt.Sprintf("%v %v, ", v.Lng, v.Lat))
	}
	// Close the loop
	b.WriteString(fmt.Sprintf("%v %v", p.Points[0].Lng, p.Points[0].Lat))
	b.WriteString("))")
	return b.String()
}

// Value implements "database/sql/driver".Valuer and will return the string
// representation of p by calling the String() method.
func (p Polygon) Value() (driver.Value, error) {
	return p.String(), nil
}

// TypeDef implements "github.com/outdoorsy/gorp".TypeDeffer and will return
// the type definition to be used when running a "CREATE TABLE" statement.
func (p Polygon) TypeDef() string {
	return fmt.Sprintf("GEOGRAPHY(POLYGON, %d)", DefaultSRID)
}

// Valid returns whether this Polygon is valid and usable.
func (p *Polygon) Valid() bool {
	return len(p.Points) >= 3
}

type withinFilter struct {
	field        interface{}
	target       Geography
	radiusMeters uint
}

func (f *withinFilter) ActualValues() []interface{} {
	return []interface{}{f.field, f.target, f.radiusMeters}
}

func (f *withinFilter) Where(values ...string) string {
	col, targetBind, radiusBind := values[0], values[1], values[2]
	return fmt.Sprintf("ST_DWithin(%s, %s, %s)", col, targetBind, radiusBind)
}

// WithinMeters is a filter that checks if a Geography is within a certain
// radius (in meters) of a a geography column.
func WithinMeters(geoFieldPtr interface{}, target Geography, radiusMeters uint) filters.Filter {
	return &withinFilter{field: geoFieldPtr, target: target, radiusMeters: radiusMeters}
}

type containsFilter struct {
	container interface{}
	target    interface{}
}

func (f *containsFilter) ActualValues() []interface{} {
	return []interface{}{f.container, f.target}
}

func (f *containsFilter) Where(values ...string) string {
	container, target := values[0], values[1]
	return fmt.Sprintf("ST_Contains(%s::geography::geometry, %s::geography::geometry)", container, target)
}

// Contains is a filter that checks if a GIS geography value is
// contained within a GIS polygon geography value.  container will be
// used as a GIS literal if it is of type Polygon, and target will be
// used as a GIS literal if it is of type Polygon or Geography.
// Otherwise, they will be used as if they are pointers to fields in a
// reference table, which map to SQL columns of GIS types.
//
// Values will be first cast to geography (to ensure their SRID is set
// properly), then cast to geometry for the comparison.
func Contains(container, target interface{}) filters.Filter {
	return &containsFilter{container: container, target: target}
}

type distanceWrapper struct {
	from interface{}
	to   interface{}
}

func (wrapper distanceWrapper) ActualValues() []interface{} {
	return []interface{}{wrapper.from, wrapper.to}
}

func (wrapper distanceWrapper) WrapSql(sqlValues ...string) string {
	if len(sqlValues) != 2 {
		panic("This should be impossible.  There are more sql values than actual values.")
	}
	return fmt.Sprintf("ST_Distance(%s, %s)", sqlValues[0], sqlValues[1])
}

// Distance wraps two Geometry arguments (or pointers to Geometry fields) in a
// call to PostGIS to get the distance (in meters) between them.
func Distance(from interface{}, to interface{}) filters.MultiSqlWrapper {
	return distanceWrapper{from: from, to: to}
}
