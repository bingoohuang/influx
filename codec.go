package influx

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	influxModels "github.com/influxdata/influxdb1-client/models"
	"github.com/mitchellh/mapstructure"
)

var (
	timeType   = reflect.TypeOf(time.Time{})
	stringType = reflect.TypeOf("")
)

// InfluxMeasurement is the const field name to tag the measurement name of the struct.
const InfluxMeasurement = "InfluxMeasurement"

func encode(d interface{}, timeField *usingValue) (point Point, err error) {
	dValue := reflect.ValueOf(d)
	if dValue.Kind() == reflect.Ptr {
		dValue = reflect.Indirect(dValue)
	}

	if dValue.Kind() != reflect.Struct {
		err = errors.New("data must be a struct")
		return
	}

	point.Tags = make(map[string]string)
	point.Fields = make(map[string]interface{})
	if timeField == nil || timeField.IsEmpty() {
		timeField = &usingValue{"time", false}
	}

	dType := dValue.Type()
	for i := 0; i < dValue.NumField(); i++ {
		fTyp, fVal := dType.Field(i), dValue.Field(i)
		if fTyp.Name == InfluxMeasurement {
			point.Measurement = fVal.String()
			continue
		}

		fieldData := getInfluxField(fTyp.Name, fTyp.Tag.Get("influx"))
		if err = point.processField(fieldData, timeField, fVal); err != nil {
			return
		}
	}

	if point.Measurement == "" {
		point.Measurement = dType.Name()
	}

	return
}

func (p *Point) processField(fieldData influxField, timeField *usingValue, f reflect.Value) error {
	fieldName := fieldData.fieldName
	if fieldName == timeField.value {
		v, ok := f.Interface().(time.Time)
		if !ok {
			return fmt.Errorf("time field %s is not time.Time", fieldName)
		}

		p.Time = v
		return nil
	}

	if fieldData.isTag {
		p.Tags[fieldName] = fmt.Sprintf("%v", f)
	}
	if fieldData.isField {
		p.Fields[fieldName] = f.Interface()
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
func decode(influxResult []influxModels.Row, result interface{}) error {
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

	metadata := &mapstructure.Metadata{}
	config := &mapstructure.DecoderConfig{
		Metadata:         metadata,
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

	if len(metadata.Unused) > 0 {
		log.Printf("D! Unused keys: %v", metadata.Unused)
	}

	return decoder.Decode(influxData)
}

// Measurement is a type that defines the influx db measurement.
type Measurement = string

type influxField struct {
	fieldName string
	isTag     bool
	isField   bool
}

func getInfluxField(fieldName, structTag string) influxField {
	fieldData := influxField{fieldName: fieldName}
	parts := strings.Split(structTag, ",")
	if fieldName, parts = parts[0], parts[1:]; fieldName != "" {
		fieldData.fieldName = fieldName
	}

	if fieldName == "-" {
		return fieldData
	}

	for _, part := range parts {
		switch part {
		case "tag":
			fieldData.isTag = true
		case "field":
			fieldData.isField = true
		}
	}

	if !fieldData.isTag {
		fieldData.isField = true
	}

	return fieldData
}
