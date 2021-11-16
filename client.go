package influx

import (
	"regexp"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

var reRemoveExtraSpace = regexp.MustCompile(`\s\s+`)

// CleanQuery can be used to strip a query string of
// newline characters. Typically, only used for debugging.
func CleanQuery(query string) string {
	ret := strings.Replace(query, "\n", "", -1)
	ret = strings.Replace(ret, "\r", "", -1)
	return reRemoveExtraSpace.ReplaceAllString(ret, " ")
}

// Cli is the client for influx encoding/decoding.
type Cli struct {
	client.Client
	Precision string

	db        string
	timeField string
}

// Point is a point for influx measurement.
type Point struct {
	Measurement string
	Time        time.Time
	Tags        map[string]string
	Fields      map[string]interface{}
}

// NewClient returns a new influx *Cli given a url, user,
// password, and precision strings.
//
// url is typically something like: http://localhost:8086
//
// precision can be ‘h’, ‘m’, ‘s’, ‘ms’, ‘u’, or ‘ns’ and is
// used during write operations.
func NewClient(url, user, passwd, precision string) (*Cli, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: user,
		Password: passwd,
	})
	if err != nil {
		return nil, err
	}

	if precision == "" {
		precision = "ns"
	}

	return &Cli{
		Precision: precision,
		Client:    c,
	}, nil
}

// NewCli create a client using a direct client.Client.
func NewCli(c client.Client) *Cli {
	return &Cli{Precision: "ns", Client: c}
}

// UseDB sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Cli) UseDB(db string) *Cli {
	c.db = db
	return c
}

// UseTimeField sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Cli) UseTimeField(fieldName string) *Cli {
	c.timeField = fieldName
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
func (c *Cli) DecodeQuery(q string, result interface{}) error {
	// sample results check website
	// https://docs.influxdata.com/influxdb/v1.7/guides/querying_data/
	response, err := c.Query(client.Query{
		Command:   q,
		Database:  c.db,
		Chunked:   false,
		ChunkSize: 100,
	})

	if err != nil {
		return err
	}
	if response.Error() != nil {
		return response.Error()
	}

	if len(response.Results) == 0 {
		return nil
	}

	return Decode(response.Results[0].Series, result)
}

// WritePoint is used to write arbitrary data into InfluxDb.
//
// data must be a struct with struct field tags that defines the names used
// in InfluxDb for each field. A "tag" tag can be added to indicate the
// struct field should be an InfluxDb tag (vs field). A tag of '-' indicates
// the struct field should be ignored. A struct field of Time is required and
// is used for the time of the sample.
func (c *Cli) WritePoint(data interface{}) error {
	point, err := Encode(data, c.timeField)
	if err != nil {
		return err
	}

	return c.WritePointRaw(point)
}

// WritePointRaw is used to write a point specifying tags and fields.
func (c *Cli) WritePointRaw(p Point) (err error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  c.db,
		Precision: c.Precision,
	})
	if err != nil {
		return err
	}

	pt, err := client.NewPoint(p.Measurement, p.Tags, p.Fields, p.Time)

	if err != nil {
		return err
	}

	bp.AddPoint(pt)

	return c.Write(bp)
}
