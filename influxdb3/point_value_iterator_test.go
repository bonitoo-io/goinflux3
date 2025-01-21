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
		{Name: "f1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer writer.Close()

	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rec := rb.NewRecord() // first record is empty
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	rec = rb.NewRecord()
	_ = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 9}, nil)
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

	var resultSet []int64
	for {
		pointValues, err := it.Next()
		if errors.Is(err, Done) {
			break
		}

		assert.NotNil(t, pointValues)
		assert.NoError(t, err)

		resultSet = append(resultSet, *pointValues.GetIntegerField("f1"))
	}
	assert.True(t, slices.Equal([]int64{1, 2, 3, 4, 5, 7, 8, 9}, resultSet))

	pointValues, err := it.Next()
	assert.Equal(t, err, Done)
	assert.Nil(t, pointValues)
}
