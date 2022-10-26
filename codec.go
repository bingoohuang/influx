package influx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/bingoohuang/gg/pkg/mapstruct"
	"github.com/bingoohuang/gg/pkg/ss"
	"github.com/bingoohuang/gg/pkg/strcase"
	"github.com/influxdata/influxdb1-client/models"
)

var (
	timeType   = reflect.TypeOf(time.Time{})
	stringType = reflect.TypeOf("")
)

// InfluxMeasurement is the const field name to tag the measurement name of the struct.
const InfluxMeasurement = "InfluxMeasurement"

// Encode encodes a d into influx Point.
func Encode(d interface{}) (p Point, err error) {
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

	dt := dv.Type()
	var times []time.Time

	for i := 0; i < dv.NumField(); i++ {
		ft, fv := dt.Field(i), dv.Field(i)
		if ft.Name == InfluxMeasurement {
			p.Measurement = fv.String()
			continue
		}

		fd := ParseInfluxTag(ft.Name, ft.Tag.Get("influx"))
		if v := fd.Properties["measurement"]; v != "" {
			p.Measurement = v
			continue
		}

		if err = p.processField(fd, fv); err != nil {
			return
		}

		if p.Time.IsZero() && fv.CanConvert(timeType) {
			if v, ok := fv.Convert(timeType).Interface().(time.Time); ok {
				times = append(times, v)
			}
		}
	}

	if p.Time.IsZero() && len(times) == 1 {
		p.Time = times[0]
	}

	if p.Measurement == "" {
		p.Measurement = dt.Name()
	}

	return
}

func (p *Point) processField(fd *Field, f reflect.Value) error {
	if fd.Name == "Time" || fd.Name == "time" {
		if v, ok := f.Interface().(time.Time); ok {
			p.Time = v
			return nil
		}
		return fmt.Errorf("time field %s is not time.Time", fd.Name)
	}

	if fd.IsTag {
		p.Tags[fd.Name] = fmt.Sprintf("%v", f)
	}
	if fd.IsField {
		p.Fields[fd.Name] = f.Interface()
	}

	return nil
}

// Decode is used to process data returned by an InfluxDb query and uses reflection
// to transform it into an array of structs of type result.
//
// This function is used internally by the Query function.
// example result layout:
//
//	{
//	   "results": [{
//	       "statement_id": 0,
//	       "series": [{
//	           "name": "cpu_load_short",
//	           "columns": ["time", "Value"],
//	           "values": [
//	               ["2015-01-29T21:55:43.702900257Z", 2],
//	               ["2015-01-29T21:55:43.702900257Z", 0.55],
//	               ["2015-06-11T20:46:02Z", 0.64]
//	           ]
//	       }]
//	   }]
//	}
func Decode(influxResult []models.Row, result interface{}) error {
	return DecodeOption(influxResult, result, &QueryOption{})
}

func DecodeOption(influxResult []models.Row, result interface{}, option *QueryOption) error {
	influxData := make([]map[string]interface{}, 0)

	for _, series := range influxResult {
		for _, v := range series.Values {
			r := make(map[string]interface{})
			for i, c := range series.Columns {
				r[c] = v[i]
				if option.tagKeys[c] {
					if cv, ok := v[i].(string); ok {
						if !ss.AnyOf(cv, (*option.ReturnTags)[c]...) {
							(*option.ReturnTags)[c] = append((*option.ReturnTags)[c], cv)
						}
					}
				}
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

	config := &mapstruct.Config{
		Metadata:   &mapstruct.Metadata{},
		Result:     result,
		TagNames:   []string{"influx"},
		WeakType:   true,
		Squash:     true,
		ZeroFields: false,
		Hook: func(f, t reflect.Type, data interface{}) (interface{}, error) {
			if t == timeType && f == stringType {
				tt, err := time.ParseInLocation(time.RFC3339, data.(string), time.UTC)
				if err == nil {
					tt = tt.In(time.Local)
				}

				return tt, err
			}

			return data, nil
		},
	}

	decoder, err := mapstruct.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(influxData)
}

type Field struct {
	Name       string
	IsTag      bool
	IsField    bool
	Properties map[string]string
}

func ParseInfluxTag(fieldName string, structTag string) *Field {
	fd := &Field{Name: fieldName, Properties: map[string]string{}}
	parts := strings.Split(structTag, ",")
	var f string
	f, parts = parts[0], parts[1:]
	f = strings.TrimSpace(f)
	if f != "" {
		fd.Name = f
	} else {
		fd.Name = strcase.ToCamelLower(fd.Name)
	}

	if fd.Name == "-" {
		return fd
	}

	for _, part := range parts {
		switch part {
		case "tag":
			fd.IsTag = true
		case "field":
			fd.IsField = true
		default:
			if kv := strings.SplitN(part, ":", 2); kv[0] != "" {
				v := ""
				if len(kv) > 1 {
					v = kv[1]
				}
				fd.Properties[kv[0]] = v
			}
		}
	}

	if !fd.IsField && !fd.IsTag && unicode.IsUpper(rune(fieldName[0])) {
		fd.IsField = true
	}

	return fd
}
