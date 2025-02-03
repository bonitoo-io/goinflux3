/*
 The MIT License

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/

package influxdb3

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
)

type responseColumnType byte

const (
	responseColumnTypeUnknown responseColumnType = iota
	responseColumnTypeTimestamp
	responseColumnTypeField
	responseColumnTypeTag
)

// QueryIterator is a custom query iterator that encapsulates and simplifies the logic for
// the flight reader. It provides methods such as Next, Value, and Index to consume the flight reader,
// or users can use the underlying reader directly with the Raw method.
//
// The QueryIterator can return responses as one of the following data types:
//   - iterator.Value() returns map[string]interface{} object representing the current row
//   - iterator.AsPoints() returns *PointValues object representing the current row
//   - iterator.Raw() returns the underlying *flight.Reader object
type QueryIterator struct {
	reader *flight.Reader
	// Current record
	record arrow.Record
	// Index of row of current object in current record
	indexInRecord int
	// Total index of current object
	i int64
	// Current object
	current map[string]interface{}
	// Done
	done bool
}

func newQueryIterator(reader *flight.Reader) *QueryIterator {
	return &QueryIterator{
		reader:        reader,
		record:        nil,
		indexInRecord: -1,
		i:             -1,
		current:       nil,
	}
}

// Next reads the next value of the flight reader and returns true if a value is present.
//
// Returns:
//   - true if a value is present, false otherwise.
func (i *QueryIterator) Next() bool {
	if i.done {
		return false
	}
	i.indexInRecord++
	i.i++
	for i.record == nil || i.indexInRecord >= int(i.record.NumRows()) {
		if !i.reader.Next() {
			i.done = true
			return false
		}
		i.record = i.reader.Record()
		i.indexInRecord = 0
	}

	readerSchema := i.reader.Schema()
	obj := make(map[string]interface{}, len(i.record.Columns()))

	for ci, col := range i.record.Columns() {
		field := readerSchema.Field(ci)
		name := field.Name
		value, _, err := getArrowValue(col, field, i.indexInRecord)
		if err != nil {
			panic(err)
		}
		obj[name] = value
	}

	i.current = obj

	return true
}

// AsPoints return data from InfluxDB v3 into PointValues structure.
func (i *QueryIterator) AsPoints() *PointValues {
	return rowToPointValue(i.record, i.indexInRecord)
}

func rowToPointValue(record arrow.Record, rowIndex int) *PointValues {
	readerSchema := record.Schema()
	p := NewPointValues("")

	for ci, col := range record.Columns() {
		field := readerSchema.Field(ci)
		name := field.Name
		value, columnType, err := getArrowValue(col, field, rowIndex)
		if err != nil {
			panic(err)
		}
		if value == nil {
			continue
		}

		if stringValue, isString := value.(string); ((name == "measurement") || (name == "iox::measurement")) && isString {
			p.SetMeasurement(stringValue)
			continue
		}

		switch {
		case columnType == responseColumnTypeUnknown:
			if timestampValue, isTimestamp := value.(arrow.Timestamp); isTimestamp && name == "time" {
				p.SetTimestamp(timestampValue.ToTime(arrow.Nanosecond))
			} else {
				p.SetField(name, value)
			}
		case columnType == responseColumnTypeField:
			p.SetField(name, value)
		case columnType == responseColumnTypeTag:
			p.SetTag(name, value.(string))
		case columnType == responseColumnTypeTimestamp:
			p.SetTimestamp(value.(time.Time))
		}
	}

	return p
}

// Value returns the current value from the flight reader as a map object.
// The map contains the fields and tags as key-value pairs.
//
// The current value types respect metadata provided by InfluxDB v3 metadata query response.
// Tags are mapped as a "string", timestamp as "time.Time", and fields as their respective types.
//
// Field are mapped to the following types:
//   - iox::column_type::field::integer: => int64
//   - iox::column_type::field::uinteger: => uint64
//   - iox::column_type::field::float: => float64
//   - iox::column_type::field::string: => string
//   - iox::column_type::field::boolean: => bool
//
// Returns:
//   - A map[string]interface{} object representing the current value.
func (i *QueryIterator) Value() map[string]interface{} {
	return i.current
}

// Index returns the current index of Value.
//
// Returns:
//   - The current index value.
func (i *QueryIterator) Index() interface{} {
	return i.i
}

// Done returns a boolean value indicating whether the iteration is complete or not.
//
// Returns:
//   - true if the iteration is complete, false otherwise.
func (i *QueryIterator) Done() bool {
	return i.done
}

// Raw returns the underlying flight.Reader associated with the QueryIterator.
// WARNING: It is imperative to use either the Raw method or the Value and Next functions, but not both at the same time,
// as it can lead to unpredictable behavior.
//
// Returns:
//   - The underlying flight.Reader.
func (i *QueryIterator) Raw() *flight.Reader {
	return i.reader
}

func getArrowValue(arrayNoType arrow.Array, field arrow.Field, i int) (any, responseColumnType, error) {
	var columnType = responseColumnTypeUnknown
	if arrayNoType.IsNull(i) {
		return nil, columnType, nil
	}
	var value any
	switch arrayNoType.DataType().ID() {
	case arrow.NULL:
		value = nil
	case arrow.BOOL:
		value = arrayNoType.(*array.Boolean).Value(i)
	case arrow.UINT8:
		value = arrayNoType.(*array.Uint8).Value(i)
	case arrow.INT8:
		value = arrayNoType.(*array.Int8).Value(i)
	case arrow.UINT16:
		value = arrayNoType.(*array.Uint16).Value(i)
	case arrow.INT16:
		value = arrayNoType.(*array.Int16).Value(i)
	case arrow.UINT32:
		value = arrayNoType.(*array.Uint32).Value(i)
	case arrow.INT32:
		value = arrayNoType.(*array.Int32).Value(i)
	case arrow.UINT64:
		value = arrayNoType.(*array.Uint64).Value(i)
	case arrow.INT64:
		value = arrayNoType.(*array.Int64).Value(i)
	case arrow.FLOAT16:
		value = arrayNoType.(*array.Float16).Value(i)
	case arrow.FLOAT32:
		value = arrayNoType.(*array.Float32).Value(i)
	case arrow.FLOAT64:
		value = arrayNoType.(*array.Float64).Value(i)
	case arrow.STRING:
		value = arrayNoType.(*array.String).Value(i)
	case arrow.BINARY:
		value = arrayNoType.(*array.Binary).Value(i)
	case arrow.FIXED_SIZE_BINARY:
		value = arrayNoType.(*array.FixedSizeBinary).Value(i)
	case arrow.DATE32:
		value = arrayNoType.(*array.Date32).Value(i)
	case arrow.DATE64:
		value = arrayNoType.(*array.Date64).Value(i)
	case arrow.TIMESTAMP:
		value = arrayNoType.(*array.Timestamp).Value(i)
	case arrow.TIME32:
		value = arrayNoType.(*array.Time32).Value(i)
	case arrow.TIME64:
		value = arrayNoType.(*array.Time64).Value(i)
	case arrow.INTERVAL_MONTHS:
		value = arrayNoType.(*array.MonthInterval).Value(i)
	case arrow.INTERVAL_DAY_TIME:
		value = arrayNoType.(*array.DayTimeInterval).Value(i)
	case arrow.DECIMAL128:
		value = arrayNoType.(*array.Decimal128).Value(i)
	case arrow.DECIMAL256:
		value = arrayNoType.(*array.Decimal256).Value(i)
	case arrow.DURATION:
		value = arrayNoType.(*array.Duration).Value(i)
	case arrow.LARGE_STRING:
		value = arrayNoType.(*array.LargeString).Value(i)
	case arrow.LARGE_BINARY:
		value = arrayNoType.(*array.LargeBinary).Value(i)
	case arrow.INTERVAL_MONTH_DAY_NANO:
		value = arrayNoType.(*array.MonthDayNanoInterval).Value(i)
	default:
		return nil, columnType, fmt.Errorf("not supported data type: %s", arrayNoType.DataType().ID().String())
	}

	if metadata, hasMetadata := field.Metadata.GetValue("iox::column::type"); hasMetadata {
		value, columnType = getMetadataType(metadata, value, columnType)
	}
	return value, columnType, nil
}

func getMetadataType(metadata string, value any, columnType responseColumnType) (any, responseColumnType) {
	switch metadata {
	case "iox::column_type::field::integer":
		if intValue, ok := value.(int64); ok {
			value = intValue
			columnType = responseColumnTypeField
		}
	case "iox::column_type::field::uinteger":
		if uintValue, ok := value.(uint64); ok {
			value = uintValue
			columnType = responseColumnTypeField
		}
	case "iox::column_type::field::float":
		if floatValue, ok := value.(float64); ok {
			value = floatValue
			columnType = responseColumnTypeField
		}
	case "iox::column_type::field::string":
		if stringValue, ok := value.(string); ok {
			value = stringValue
			columnType = responseColumnTypeField
		}
	case "iox::column_type::field::boolean":
		if boolValue, ok := value.(bool); ok {
			value = boolValue
			columnType = responseColumnTypeField
		}
	case "iox::column_type::tag":
		if stringValue, ok := value.(string); ok {
			value = stringValue
			columnType = responseColumnTypeTag
		}
	case "iox::column_type::timestamp":
		if timestampValue, ok := value.(arrow.Timestamp); ok {
			value = timestampValue.ToTime(arrow.Nanosecond)
			columnType = responseColumnTypeTimestamp
		}
	}
	return value, columnType
}
