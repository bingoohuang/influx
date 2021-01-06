package influx

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	influxClient "github.com/influxdata/influxdb1-client/v2"
)

var reRemoveExtraSpace = regexp.MustCompile(`\s\s+`)

// CleanQuery can be used to strip a query string of
// newline characters. Typically only used for debugging.
func CleanQuery(query string) string {
	ret := strings.Replace(query, "\n", "", -1)
	return reRemoveExtraSpace.ReplaceAllString(ret, " ")
}

// Client is the client for influx encoding/decoding.
type Client struct {
	influxClient.Client
	precision string

	db          usingValue
	measurement usingValue
	timeField   usingValue
}

// Point is a point for influx measurement.
type Point struct {
	Measurement string
	Time        time.Time
	Tags        map[string]string
	Fields      map[string]interface{}
}

// Retain tries to retain the temp data.
func (c *Client) Retain() {
	c.db.Retain()
	c.measurement.Retain()
	c.timeField.Retain()
}

type usingValue struct {
	value  string
	retain bool
}

// Retain ...
func (u *usingValue) Retain() {
	if !u.retain {
		u.value = ""
	}
}

// IsEmpty tests if the value is empty.
func (u *usingValue) IsEmpty() bool {
	return u.value == ""
}

// NewClient returns a new influx *Client given a url, user,
// password, and precision strings.
//
// url is typically something like: http://localhost:8086
//
// precision can be ‘h’, ‘m’, ‘s’, ‘ms’, ‘u’, or ‘ns’ and is
// used during write operations.
func NewClient(url, user, passwd, precision string) (*Client, error) {
	client, err := influxClient.NewHTTPClient(influxClient.HTTPConfig{
		Addr:     url,
		Username: user,
		Password: passwd,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		precision: precision,
		Client:    client,
	}, nil
}

// UseDB sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Client) UseDB(db string) *Client {
	c.db = usingValue{value: db, retain: true}
	return c
}

// UseMeasurement sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Client) UseMeasurement(measurement string) *Client {
	c.measurement = usingValue{value: measurement, retain: true}
	return c
}

// UseTimeField sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Client) UseTimeField(fieldName string) *Client {
	c.timeField = usingValue{value: fieldName, retain: true}
	return c
}

// DecodeQuery executes an InfluxDb query, and unpacks the result into the
// result data structure.
//
// result must be an array of structs that contains the fields returned
// by the query. The struct type must always contain a Time field. The
// struct type must also include influx field tags which map the struct
// field name to the InfluxDb field/tag names. This tag is currently
// required as typically Go struct field names start with a capital letter,
// and InfluxDb field/tag names typically start with a lower case letter.
// The struct field tag can be set to '-' which indicates this field
// should be ignored.
func (c *Client) DecodeQuery(q string, result interface{}) error {
	if c.db.IsEmpty() {
		return fmt.Errorf("no db set for query")
	}

	// sample results check website
	// https://docs.influxdata.com/influxdb/v1.7/guides/querying_data/
	response, err := c.Query(influxClient.Query{
		Command:   q,
		Database:  c.db.value,
		Chunked:   false,
		ChunkSize: 100,
	})
	c.Retain()

	if err != nil {
		return err
	}

	if response.Error() != nil {
		return response.Error()
	}

	results := response.Results
	if len(results) == 0 {
		return nil
	}

	return decode(results[0].Series, result)
}

// WritePoint is used to write arbitrary data into InfluxDb.
//
// data must be a struct with struct field tags that defines the names used
// in InfluxDb for each field. A "tag" tag can be added to indicate the
// struct field should be an InfluxDb tag (vs field). A tag of '-' indicates
// the struct field should be ignored. A struct field of Time is required and
// is used for the time of the sample.
func (c *Client) WritePoint(data interface{}) error {
	if c.db.IsEmpty() {
		return fmt.Errorf("no db set for query")
	}

	point, err := encode(data, &c.timeField)
	if err != nil {
		return err
	}

	return c.WritePointRaw(point)
}

// WritePointRaw is used to write a point specifying tags and fields.
func (c *Client) WritePointRaw(p Point) (err error) {
	if c.db.IsEmpty() {
		return fmt.Errorf("no db set for query")
	}

	bp, err := influxClient.NewBatchPoints(influxClient.BatchPointsConfig{
		Database:  c.db.value,
		Precision: c.precision,
	})
	if err != nil {
		return err
	}

	pt, err := influxClient.NewPoint(p.Measurement, p.Tags, p.Fields, p.Time)
	c.Retain()

	if err != nil {
		return err
	}

	bp.AddPoint(pt)

	return c.Write(bp)
}
