package influx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/models"
	"github.com/mitchellh/mapstructure"
)

var (
	timeType   = reflect.TypeOf(time.Time{})
	stringType = reflect.TypeOf("")
)

// InfluxMeasurement is the const field name to tag the measurement name of the struct.
const InfluxMeasurement = "InfluxMeasurement"

// Encode encodes a d into influx Point.
func Encode(d interface{}, timeField *usingValue) (p Point, err error) {
	dv := reflect.ValueOf(d)
	if dv.Kind() == reflect.Ptr {
		dv = reflect.Indirect(dv)
	}

	if dv.Kind() != reflect.Struct {
		err = errors.New("data must be a struct")
		return
	}

	p.Tags = make(map[string]string)
	p.Fields = make(map[string]interface{})
	if timeField == nil || timeField.IsEmpty() {
		timeField = &usingValue{"time", false}
	}

	dt := dv.Type()
	for i := 0; i < dv.NumField(); i++ {
		ft, fv := dt.Field(i), dv.Field(i)
		if ft.Name == InfluxMeasurement {
			p.Measurement = fv.String()
			continue
		}

		fd := parseInfluxTag(ft.Name, ft.Tag.Get("influx"))
		if err = p.processField(fd, timeField, fv); err != nil {
			return
		}
	}

	if p.Measurement == "" {
		p.Measurement = dt.Name()
	}

	return
}

func (p *Point) processField(fd influxField, timeField *usingValue, f reflect.Value) error {
	if fd.fieldName == timeField.value {
		v, ok := f.Interface().(time.Time)
		if !ok {
			return fmt.Errorf("time field %s is not time.Time", fd.fieldName)
		}

		p.Time = v
		return nil
	}

	if fd.isTag {
		p.Tags[fd.fieldName] = fmt.Sprintf("%v", f)
	}
	if fd.isField {
		p.Fields[fd.fieldName] = f.Interface()
	}

	return nil
}

// Decode is used to process data returned by an InfluxDb query and uses reflection
// to transform it into an array of structs of type result.
//
// This function is used internally by the Query function.
// example result layout:
// {
//    "results": [{
//        "statement_id": 0,
//        "series": [{
//            "name": "cpu_load_short",
//            "columns": ["time", "value"],
//            "values": [
//                ["2015-01-29T21:55:43.702900257Z", 2],
//                ["2015-01-29T21:55:43.702900257Z", 0.55],
//                ["2015-06-11T20:46:02Z", 0.64]
//            ]
//        }]
//    }]
//}
func Decode(influxResult []models.Row, result interface{}) error {
	influxData := make([]map[string]interface{}, 0)

	for _, series := range influxResult {
		for _, v := range series.Values {
			r := make(map[string]interface{})
			for i, c := range series.Columns {
				r[c] = v[i]
			}
			for tag, val := range series.Tags {
				r[tag] = val
			}
			r[InfluxMeasurement] = series.Name

			influxData = append(influxData, r)
		}
	}

	if len(influxData) == 0 {
		return nil
	}

	config := &mapstructure.DecoderConfig{
		Metadata:         &mapstructure.Metadata{},
		Result:           result,
		TagName:          "influx",
		WeaklyTypedInput: false,
		ZeroFields:       false,
		DecodeHook: func(f, t reflect.Type, data interface{}) (interface{}, error) {
			if t == timeType && f == stringType {
				return time.Parse(time.RFC3339, data.(string))
			}

			return data, nil
		},
	}

	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(influxData)
}

type influxField struct {
	fieldName string
	isTag     bool
	isField   bool
}

func parseInfluxTag(fieldName, structTag string) influxField {
	fd := influxField{fieldName: fieldName}
	parts := strings.Split(structTag, ",")
	if fieldName, parts = parts[0], parts[1:]; fieldName != "" {
		fd.fieldName = fieldName
	}

	if fieldName == "-" {
		return fd
	}

	for _, part := range parts {
		switch part {
		case "tag":
			fd.isTag = true
		case "field":
			fd.isField = true
		}
	}

	if !fd.isTag {
		fd.isField = true
	}

	return fd
}
