package influx

import (
	"log"
	"regexp"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

var reRemoveExtraSpace = regexp.MustCompile(`\s\s+`)

// CleanQuery can be used to strip a query string of
// newline characters. Typically, only used for debugging.
func CleanQuery(query string) string {
	ret := strings.Replace(query, "\n", " ", -1)
	ret = strings.Replace(ret, "\r", " ", -1)
	return reRemoveExtraSpace.ReplaceAllString(ret, " ")
}

// Cli is the client for influx encoding/decoding.
type Cli struct {
	client.Client
	Precision string

	db string
}

// Point is a point for influx measurement.
type Point struct {
	Measurement string
	Time        time.Time
	Tags        map[string]string
	Fields      map[string]interface{}
}

type Config struct {
	User      string
	Password  string
	Precision string
	Addr      string
	Client    client.Client
}

// WithAddr set Addr which typically like: http://localhost:8086.
func WithAddr(addr string) ConfigFn { return func(c *Config) { c.Addr = addr } }

func WithUser(user, password string) ConfigFn {
	return func(c *Config) {
		c.User = user
		c.Password = password
	}
}

// WithClient create a client using a direct client.Client.
func WithClient(client client.Client) ConfigFn { return func(c *Config) { c.Client = client } }

// WithPrecision set precision which can be ‘h’, ‘m’, ‘s’, ‘ms’, ‘u’, or ‘ns’ and is used during write operations.
func WithPrecision(precision string) ConfigFn { return func(c *Config) { c.Precision = precision } }

type ConfigFn func(*Config)

// New returns a new influx *Cli.
func New(fns ...ConfigFn) (*Cli, error) {
	c := &Config{Addr: "http://localhost:8086", Precision: "ns"}
	for _, fn := range fns {
		fn(c)
	}

	if c.Client == nil {
		var err error
		if c.Client, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     c.Addr,
			Username: c.User, Password: c.Password,
		}); err != nil {
			return nil, err
		}
	}

	return &Cli{Precision: c.Precision, Client: c.Client}, nil
}

// UseDB sets the DB to use for Query, WritePoint, and WritePointTagsFields.
func (c *Cli) UseDB(db string) *Cli {
	c.db = db
	return c
}

// QueryOption defines the options for querying.
type QueryOption struct {
	ReturnTags           *map[string][]string
	ReturnTagValuesLimit int
	tagKeys              map[string]bool
}

// QueryOptionFn defines the option func.
type QueryOptionFn func(*QueryOption)

// WithTagsReturn specifying the tag values should be returned.
func WithTagsReturn(tags *map[string][]string, valuesLimit int) QueryOptionFn {
	return func(q *QueryOption) {
		q.ReturnTags = tags
		q.ReturnTagValuesLimit = valuesLimit
	}
}

// DecodeQuery executes an InfluxDb query, and unpacks the result into the result data structure.
//
// result must be an array of structs that contains the fields returned by the query. The struct
// type must always contain a Time field. The struct type must also include influx field tags
// which map the struct field name to the InfluxDb field/tag names. This tag is currently
// required as typically Go struct field names start with a capital letter, and InfluxDb field/tag
// names typically start with a lower case letter. The struct field tag can be set to '-' which
// indicates this field should be ignored.

func (c *Cli) DecodeQuery(q string, result interface{}, options ...QueryOptionFn) error {
	option := &QueryOption{}
	for _, f := range options {
		f(option)
	}

	// sample results check website
	// https://docs.influxdata.com/influxdb/v1.7/guides/querying_data/
	cq := client.Query{
		Command:   q,
		Database:  c.db,
		Chunked:   false,
		ChunkSize: 100,
	}
	response, err := c.Query(cq)
	if err != nil {
		return err
	}
	if response.Error() != nil {
		return response.Error()
	}

	if len(response.Results) == 0 {
		return nil
	}

	series := response.Results[0].Series

	if option.ReturnTags != nil {
		if option.tagKeys, err = c.queryTagKeys(&cq, series); err != nil {
			log.Printf("query tag keys failed: %v", err)
		}
	}

	return DecodeOption(series, result, option)
}

// WritePoint is used to write arbitrary data into InfluxDb.
//
// data must be a struct with struct field tags that defines the names used
// in InfluxDb for each field. A "tag" tag can be added to indicate the
// struct field should be an InfluxDb tag (vs field). A tag of '-' indicates
// the struct field should be ignored. A struct field of Time is required and
// is used for the time of the sample.
func (c *Cli) WritePoint(data interface{}) error {
	point, err := Encode(data)
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
