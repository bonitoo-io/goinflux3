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
	"errors"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"time"
)

var Done = errors.New("no more items in iterator")

// PointValueIterator is a custom query iterator that encapsulates and simplifies the logic for
// the flight reader. It provides method Next to consume the flight reader,
//
// The PointValueIterator Next function will return response as a *PointValues object representing the current row
type PointValueIterator struct {
	reader *flight.Reader
	// Index of row of current object in current record
	index int
	// Current record
	record arrow.Record
}

// Return a new PointValueIterator
func newPointValueIterator(reader *flight.Reader) *PointValueIterator {
	return &PointValueIterator{
		reader: reader,
		index:  -1,
		record: nil,
	}
}

// Next returns the next result.
// Its second return value is iterator.Done if there are no more results.
// Once Next returns Done in the second parameter, all subsequent calls will return Done.
//
//	it := newPointValueIterator(flightReader)
//	for {
//		PointValue, err := it.Next()
//		if err == iterator.Done {
//			break
//		}
//		if err != nil {
//			return err
//		}
//		process(PointValue)
//	}
func (it *PointValueIterator) Next() (*PointValues, error) {
	it.index++

	for it.record == nil || it.index >= int(it.record.NumRows()) {
		if !it.reader.Next() {
			return nil, Done
		}
		it.record = it.reader.Record()
		it.index = 0
	}

	pointValues, err := asPoints(it.reader.Record(), it.index)
	if err != nil {
		return nil, err
	}

	return pointValues, nil
}

// Index return the current index of PointValueIterator
func (it *PointValueIterator) Index() int {
	return it.index
}

func asPoints(record arrow.Record, index int) (*PointValues, error) {
	readerSchema := record.Schema()
	p := NewPointValues("")

	for ci, col := range record.Columns() {
		field := readerSchema.Field(ci)
		name := field.Name
		value, columnType, err := getArrowValue(col, field, index)
		if err != nil {
			return nil, err
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

	return p, nil
}
