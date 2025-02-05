//go:build e2e
// +build e2e

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

package influxdb3_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3/batching"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SkipCheck(t *testing.T) {
	if _, present := os.LookupEnv("TESTING_INFLUXDB_URL"); !present {
		t.Skip("TESTING_INFLUXDB_URL not set")
	}
	if _, present := os.LookupEnv("TESTING_INFLUXDB_TOKEN"); !present {
		t.Skip("TESTING_INFLUXDB_TOKEN not set")
	}
	if _, present := os.LookupEnv("TESTING_INFLUXDB_DATABASE"); !present {
		t.Skip("TESTING_INFLUXDB_DATABASE not set")
	}
}

func TestWriteAndQueryExample(t *testing.T) {
	SkipCheck(t)
	now := time.Now().UTC()
	testId := now.UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer client.Close()

	tableName := "weather"
	tagKey := "location"
	tagValue := "sun-valley-1"

	// Write test

	p := influxdb3.NewPointWithMeasurement(tableName).
		SetTag(tagKey, tagValue).
		SetField("temp", 15.5).
		SetField("index", 80).
		SetField("uindex", uint64(800)).
		SetField("valid", true).
		SetField("testId", testId).
		SetField("text", "a1").
		SetTimestamp(now)
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)

	sensorData := struct {
		Table  string    `lp:"measurement"`
		Loc    string    `lp:"tag,location"`
		Temp   float64   `lp:"field,temp"`
		Index  int64     `lp:"field,index"`
		UIndex uint64    `lp:"field,uindex"`
		Valid  bool      `lp:"field,valid"`
		TestId int64     `lp:"field,testId"`
		Text   string    `lp:"field,text"`
		Time   time.Time `lp:"timestamp"`
	}{tableName, tagValue, 24.5, -15, uint64(150), false, testId, "b1", now.Add(1 * time.Second)}
	err = client.WriteData(context.Background(), []any{sensorData})
	require.NoError(t, err)

	// SQL query test

	query := fmt.Sprintf(`
		SELECT *
		FROM "%s"
		WHERE
		time >= now() - interval '10 minute'
		AND
		"%s" = '%s'
		AND
		"testId" = %d
		ORDER BY time
	`, tableName, tagKey, tagValue, testId)

	// retry query few times until data updates
	sleepTime := 2 * time.Second
	time.Sleep(sleepTime)
	iterator, err := client.Query(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, iterator)

	// row #1

	hasValue := iterator.Next()
	assert.True(t, hasValue)
	value := iterator.Value()
	assert.Equal(t, tagValue, value[tagKey])
	assert.Equal(t, 15.5, value["temp"])
	assert.Equal(t, int64(80), value["index"])
	assert.Equal(t, uint64(800), value["uindex"])
	assert.Equal(t, true, value["valid"])
	assert.Equal(t, "a1", value["text"])
	assert.Equal(t, now, value["time"])

	// row #2

	hasValue = iterator.Next()
	assert.True(t, hasValue)
	value = iterator.Value()
	assert.Equal(t, tagValue, value[tagKey])
	assert.Equal(t, 24.5, value["temp"])
	assert.Equal(t, int64(-15), value["index"])
	assert.Equal(t, uint64(150), value["uindex"])
	assert.Equal(t, false, value["valid"])
	assert.Equal(t, "b1", value["text"])
	assert.Equal(t, now.Add(1*time.Second), value["time"])

	assert.False(t, iterator.Done())

	assert.False(t, iterator.Next())
	assert.True(t, iterator.Done())

	iterator, err = client.Query(context.Background(), query)
	hasValue = iterator.Next()
	assert.True(t, hasValue)
	points := iterator.AsPoints()
	assert.Equal(t, uint64(800), points.Fields["uindex"])

	hasValue = iterator.Next()
	assert.True(t, hasValue)

	newPoint, _ := points.AsPointWithMeasurement("to_write")
	assert.True(t, newPoint != nil)
}

func TestQueryWithParameters(t *testing.T) {
	SkipCheck(t)
	now := time.Now().UTC()
	testId := now.UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer client.Close()

	p := influxdb3.NewPointWithMeasurement("weather").
		SetTag("location", "sun-valley-1").
		SetField("temp", 15.5).
		SetField("index", 80).
		SetField("uindex", uint64(800)).
		SetField("valid", true).
		SetField("testId", testId).
		SetField("text", "a1").
		SetTimestamp(now)
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)

	query := `
		SELECT *
		FROM weather
		WHERE
		time >= now() - interval '10 minute'
		AND
		location = $location
		AND
		"testId" = $testId
		ORDER BY time
	`
	parameters := influxdb3.QueryParameters{
		"location": "sun-valley-1",
		"testId":   testId,
	}

	sleepTime := 5 * time.Second
	time.Sleep(sleepTime)

	iterator, err := client.QueryWithParameters(context.Background(), query, parameters)
	require.NoError(t, err)
	require.NotNil(t, iterator)

	hasValue := iterator.Next()
	assert.True(t, hasValue)

	value := iterator.Value()
	assert.Equal(t, "sun-valley-1", value["location"])
	assert.Equal(t, 15.5, value["temp"])
	assert.Equal(t, int64(80), value["index"])
	assert.Equal(t, uint64(800), value["uindex"])
	assert.Equal(t, true, value["valid"])
	assert.Equal(t, "a1", value["text"])
	assert.Equal(t, now, value["time"])

	assert.False(t, iterator.Done())
	assert.False(t, iterator.Next())
	assert.True(t, iterator.Done())
}

func TestQueryPointValueWithParameters(t *testing.T) {
	SkipCheck(t)
	now := time.Now().UTC()
	testId := now.UnixNano()

	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer client.Close()

	p := influxdb3.NewPointWithMeasurement("weather5").
		SetField("text", "a1").
		SetField("testId", testId).
		SetTimestamp(now)
	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)

	query := `
		SELECT *
		FROM weather5
		WHERE
		time >= now() - interval '10 minute'
		AND
		text = $text
		AND
		"testId" = $testId
		ORDER BY time
	`
	parameters := influxdb3.QueryParameters{
		"text":   "a1",
		"testId": testId,
	}

	sleepTime := 5 * time.Second
	time.Sleep(sleepTime)

	pointValueIterator, err := client.QueryPointValueWithParameters(context.Background(), query, parameters)
	require.NoError(t, err)
	require.NotNil(t, pointValueIterator)

	PointValue, err := pointValueIterator.Next()
	assert.NoError(t, err)
	assert.NotNil(t, PointValue)
	assert.Equal(t, PointValue.GetField("text"), "a1")

	PointValue, err = pointValueIterator.Next()
	assert.Equal(t, influxdb3.Done, errors.New("no more items in iterator"))
	assert.Nil(t, PointValue)
}

func TestQueryDatabaseDoesNotExist(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: "does not exist",
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "bucket \"does not exist\" not found")
}

func TestQuerySchema(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES")
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestQuerySchemaWithOptions(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: "does not exist",
	})

	iterator, err := client.Query(context.Background(), "SHOW TABLES", influxdb3.WithDatabase(database))
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestQuerySchemaInfluxQL(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})

	iterator, err := client.Query(context.Background(), "SHOW MEASUREMENTS", influxdb3.WithQueryType(influxdb3.InfluxQL))
	require.NoError(t, err)
	assert.NotNil(t, iterator.Raw())
}

func TestWriteError(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)

	err = client.Write(context.Background(), []byte("test,type=negative val="))
	require.Error(t, err)
	assert.NotPanics(t, func() { _ = err.(*influxdb3.ServerError) })
	assert.Regexp(t, "[0-9a-f]{16}", err.(*influxdb3.ServerError).Headers["Trace-Id"][0])
	b, perr := strconv.ParseBool(err.(*influxdb3.ServerError).Headers["Trace-Sampled"][0])
	require.NoError(t, perr)
	assert.False(t, b)
	assert.NotNil(t, err.(*influxdb3.ServerError).Headers["Strict-Transport-Security"])
	assert.Regexp(t, "[0-9a-f]{32}", err.(*influxdb3.ServerError).Headers["X-Influxdb-Request-Id"][0])
	assert.NotNil(t, err.(*influxdb3.ServerError).Headers["X-Influxdb-Build"][0])

}

func TestEscapedStringValues(t *testing.T) {
	SkipCheck(t)
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	p := influxdb3.NewPoint("escapee",
		map[string]string{
			"tag1": "new\nline and space",
			"tag2": "escaped\\nline and space",
			"tag3": "escaped\nline and\ttab",
			"tag4": "preescaped\\nline and\\ttab",
		},
		map[string]interface{}{
			"fVal": 41.3,
			"sVal": "greetings\nearthlings",
		}, time.Now())

	err = client.WritePoints(context.Background(), []*influxdb3.Point{p})
	require.NoError(t, err)
	qit, err := client.Query(context.Background(), "SELECT * FROM \"escapee\" WHERE time >= now() - interval '1 minute'")
	require.NoError(t, err)
	for qit.Next() {
		assert.EqualValues(t, "greetings\\nearthlings", qit.Value()["sVal"])
		assert.EqualValues(t, "new\\nline and space", qit.Value()["tag1"])
		assert.EqualValues(t, "escaped\\nline and space", qit.Value()["tag2"])
		assert.EqualValues(t, "escaped\\nline and\\ttab", qit.Value()["tag3"])
		assert.EqualValues(t, "preescaped\\nline and\\ttab", qit.Value()["tag4"])
	}
}

func PointFromLineProtocol(lp string) (*influxdb3.Point, error) {
	groups := strings.Split(strings.TrimSpace(lp), " ")
	head := strings.Split(groups[0], ",")
	fieldLines := strings.Split(groups[1], ",")

	if len(head) < 1 {
		return nil, fmt.Errorf("invalid line format: %s", lp)
	}

	result := influxdb3.NewPointWithMeasurement(head[0])

	if len(fieldLines) < 1 {
		return nil, fmt.Errorf("LineProtocol has no fields: %s", lp)
	}

	if len(head) > 1 {
		for i := 1; i < len(head); i++ {
			tkv := strings.Split(head[i], "=")
			result = result.SetTag(tkv[0], tkv[1])
		}
	}

	for _, fl := range fieldLines {
		fkv := strings.Split(fl, "=")
		switch {
		case strings.Contains(fkv[1], "\""):
			result.SetStringField(fkv[0], fkv[1])
		case strings.Contains(fkv[1], "i"):
			fkv[1] = strings.ReplaceAll(fkv[1], "i", "")
			ival, err := strconv.ParseInt(fkv[1], 10, 64)
			if err != nil {
				return nil, err
			}
			result = result.SetField(fkv[0], ival)
		default:
			fval, err := strconv.ParseFloat(fkv[1], 64)
			if err != nil {
				return nil, err
			}
			result = result.SetField(fkv[0], fval)
		}
	}

	if len(groups[2]) > 0 {
		timestamp, err := strconv.ParseInt(groups[2], 10, 64)
		nanoFactor := int64(19 - len(groups[2]))
		timestamp *= int64(math.Pow(10.0, float64(nanoFactor)))
		if err != nil {
			return nil, fmt.Errorf("invalid time format: %s -> %w", lp, err)
		}
		result = result.SetTimestampWithEpoch(timestamp)
		result = result.SetTimestamp(result.Values.Timestamp.UTC())
	}

	return result, nil
}

// LooseComparePointValues attempts to compare values only but not exact types
// Some value types get coerced in client-server transactions
func LooseEqualPointValues(pvA *influxdb3.PointValues, pvB *influxdb3.PointValues) bool {
	if pvA.MeasurementName != pvB.MeasurementName {
		return false
	}
	if pvA.Timestamp != pvB.Timestamp {
		return false
	}
	for tagName := range pvA.Tags {
		if pvA.Tags[tagName] != pvB.Tags[tagName] {
			return false
		}
	}
	for fieldName := range pvA.Fields {
		switch pvA.Fields[fieldName].(type) {
		case int, int16, int32, int64:
			ai, aiok := pvA.Fields[fieldName].(int64)
			bi, biok := pvB.Fields[fieldName].(int64)
			if !aiok || !biok {
				return false
			}
			if ai != bi {
				return false
			}
		case float32, float64:
			af, afok := pvA.Fields[fieldName].(float64)
			bf, bfok := pvB.Fields[fieldName].(float64)
			if !afok || !bfok {
				return false
			}
			if af != bf {
				return false
			}
		default: // compare as strings
			as, saok := pvA.Fields[fieldName].(string)
			bs, sbok := pvB.Fields[fieldName].(string)
			if !saok || !sbok {
				return false
			}
			if as != bs {
				return false
			}
		}
	}
	return true
}

func TestLPBatcher(t *testing.T) {
	SkipCheck(t)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	url := os.Getenv("TESTING_INFLUXDB_URL")
	token := os.Getenv("TESTING_INFLUXDB_TOKEN")
	database := os.Getenv("TESTING_INFLUXDB_DATABASE")

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	require.NoError(t, err)
	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			slog.Warn("Failed to close client correctly.")
		}
	}(client)

	measurement := fmt.Sprintf("ibot%d", rnd.Int63n(99000)+1000)
	dataTemplate := "%s,location=%s,id=%s fVal=%f,count=%di %d"
	locations := []string{"akron", "dakar", "kyoto", "perth"}
	ids := []string{"R2D2", "C3PO", "ROBBIE"}
	lines := make([]string, 0)
	now := time.Now().UnixMilli()
	estBytesCt := 0
	lineCount := 2000
	for n := range lineCount {
		lines = append(lines, fmt.Sprintf(dataTemplate,
			measurement,
			locations[n%len(locations)],
			ids[n%len(ids)],
			(rnd.Float64()*100)-50.0, n+1, now-int64(n*1000)))
		if n%2 == 0 {
			lines[n] += "\n" // verify appending LF with every second rec
		} else {
			estBytesCt++ // LPBatcher appends missing "\n" on odd cases so increase estimate
		}
		estBytesCt += len([]byte(lines[n]))
	}

	size := 4096
	capacity := size * 2
	readyCt := 0
	emitCt := 0
	results := make([]byte, 0)
	lag := 0
	lpb := batching.NewLPBatcher(
		batching.WithBufferSize(size),
		batching.WithBufferCapacity(capacity),
		batching.WithByteEmitReadyCallback(func() {
			readyCt++
		}),
		batching.WithEmitBytesCallback(func(ba []byte) {
			emitCt++
			// N.B. LPBatcher emits up to last '\n' in packet so will usually be less than `size`
			// lag collects the difference for asserts below
			lag += size - len(ba)
			results = append(results, ba...)
			err := client.Write(context.Background(), ba, influxdb3.WithPrecision(lineprotocol.Millisecond))
			if err != nil {
				assert.Fail(t, "Failed to write ba")
			}
		}))

	sent := 0
	for n := range lines {
		if n%100 == 0 {
			lpb.Add(lines[sent : sent+100]...)
			sent += 100
		}
	}
	lpb.Add(lines[sent:]...) // add remainder

	// Check that collected emits make sense
	assert.Equal(t, readyCt, emitCt)
	assert.Equal(t, estBytesCt+lag, size*emitCt+lpb.CurrentLoadSize())

	// emit anything left over
	leftover := lpb.Emit()
	assert.Zero(t, lpb.CurrentLoadSize())
	err = client.Write(context.Background(), leftover, influxdb3.WithPrecision(lineprotocol.Millisecond))
	if err != nil {
		assert.Fail(t, "Failed to write leftover bytes from lpb - LPBatcher")
	}
	results = append(results, leftover...)

	// Retrieve and check results
	query := fmt.Sprintf("SELECT * FROM \"%s\" WHERE time >= now() - interval '90 minutes' Order by count",
		measurement)

	qiterator, qerr := client.Query(context.Background(), query)

	if qerr != nil {
		assert.Failf(t, "Failed to query.", "query: %s", query)
	}

	var pvResults []*influxdb3.PointValues
	for qiterator.Next() {
		pvResults = append(pvResults, qiterator.AsPoints())
	}

	// Check random retrieved samples match source LineProtocol
	samples := 10
	for n := range samples {
		index := 0
		if n > 0 { // always test first value
			index = rnd.Intn(len(lines))
		}
		if n == (samples - 1) {
			index = len(lines) - 1 // always test last value
		}
		point, cnvErr := PointFromLineProtocol(lines[index])
		if cnvErr != nil {
			assert.Failf(t, "Failed to deserialize point", "index: %d, line: %d", index, lines[index])
		}
		if point != nil {
			point.Values.MeasurementName = ""
			assert.True(t, LooseEqualPointValues(point.Values, pvResults[index]))
		} else {
			assert.Fail(t, "Nil returned on deserialize point", "index: %d, line: %d", index, lines[index])
		}
	}
}
