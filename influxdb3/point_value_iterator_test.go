package influxdb3

import (
	"bytes"
	"errors"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
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
	err := writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	rec = rb.NewRecord()
	err = writer.Write(rec)

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 9}, nil)
	rec = rb.NewRecord()
	err = writer.Write(rec)

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
