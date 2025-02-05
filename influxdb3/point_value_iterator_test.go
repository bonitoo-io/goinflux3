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
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestPointValueIterator(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f1", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int8},
		{Name: "f3", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "f4", Type: arrow.PrimitiveTypes.Int16},
		{Name: "f5", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rec := rb.NewRecord() // first record is empty
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{0}, nil)
	rb.Field(1).(*array.Uint8Builder).AppendValues([]uint8{1}, nil)
	rb.Field(2).(*array.Int8Builder).AppendValues([]int8{2}, nil)
	rb.Field(3).(*array.Uint16Builder).AppendValues([]uint16{3}, nil)
	rb.Field(4).(*array.Int16Builder).AppendValues([]int16{4}, nil)
	rb.Field(5).(*array.Uint32Builder).AppendValues([]uint32{5}, nil)
	rec = rb.NewRecord()
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{0}, nil)
	rb.Field(1).(*array.Uint8Builder).AppendValues([]uint8{1}, nil)
	rb.Field(2).(*array.Int8Builder).AppendValues([]int8{2}, nil)
	rb.Field(3).(*array.Uint16Builder).AppendValues([]uint16{3}, nil)
	rb.Field(4).(*array.Int16Builder).AppendValues([]int16{4}, nil)
	rb.Field(5).(*array.Uint32Builder).AppendValues([]uint32{5}, nil)

	rec = rb.NewRecord()
	_ = writer.Write(rec)

	reader := ipc.NewMessageReader(&buf)

	ipcReader, err := ipc.NewReaderFromMessageReader(
		&testMessagesReader{
			r: reader,
		})
	assert.NoError(t, err)

	fReader := &flight.Reader{Reader: ipcReader}
	it := newPointValueIterator(fReader)

	var resultSet0 []int64
	var resultSet1 []interface{}
	var resultSet2 []interface{}
	var resultSet3 []interface{}
	var resultSet4 []interface{}
	var resultSet5 []interface{}

	for {
		pointValues, err := it.Next()
		if errors.Is(err, Done) {
			break
		}
		assert.NotNil(t, pointValues)
		assert.NoError(t, err)

		resultSet0 = append(resultSet0, *pointValues.GetIntegerField("f0"))
		resultSet1 = append(resultSet1, pointValues.GetField("f1"))
		resultSet2 = append(resultSet2, pointValues.GetField("f2"))
		resultSet3 = append(resultSet3, pointValues.GetField("f3"))
		resultSet4 = append(resultSet4, pointValues.GetField("f4"))
		resultSet5 = append(resultSet5, pointValues.GetField("f5"))
	}

	assert.True(t, slices.Equal([]int64{0, 0}, resultSet0))

	assert.True(t, resultSet1[0] == uint8(1))
	assert.True(t, resultSet1[1] == uint8(1))

	assert.True(t, resultSet2[0] == int8(2))
	assert.True(t, resultSet2[1] == int8(2))

	assert.True(t, resultSet3[0] == uint16(3))
	assert.True(t, resultSet3[1] == uint16(3))

	assert.True(t, resultSet4[0] == int16(4))
	assert.True(t, resultSet4[1] == int16(4))

	assert.True(t, resultSet5[0] == uint32(5))
	assert.True(t, resultSet5[1] == uint32(5))

	pointValues, err := it.Next()
	assert.Equal(t, 2, it.Index())
	assert.Equal(t, err, Done)
	assert.Nil(t, pointValues)
}
