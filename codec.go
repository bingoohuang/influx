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

func encode(d interface{}, timeField *usingValue) (
	t time.Time,
	tags map[string]string,
	fields map[string]interface{},
	measurement string,
	err error,
) {
	tags = make(map[string]string)
	fields = make(map[string]interface{})
	dValue := reflect.ValueOf(d)

	if dValue.Kind() == reflect.Ptr {
		dValue = reflect.Indirect(dValue)
	}

	if dValue.Kind() != reflect.Struct {
		err = errors.New("data must be a struct")
		return
	}

	if timeField == nil {
		timeField = &usingValue{"time", false}
	}

	for i := 0; i < dValue.NumField(); i++ {
		f := dValue.Field(i)
		structFieldName := dValue.Type().Field(i).Name
		if structFieldName == InfluxMeasurement {
			measurement = f.String()
			continue
		}

		fieldTag := dValue.Type().Field(i).Tag.Get("influx")
		fieldData := getInfluxFieldTagData(structFieldName, fieldTag)
		fieldName := fieldData.fieldName
		if fieldName == "-" {
			continue
		}

		if fieldName == timeField.value {
			v, ok := f.Interface().(time.Time)
			if !ok {
				err = fmt.Errorf("time field %s is not time.Time", fieldName)
				return
			}

			t = v
			continue
		}

		if fieldData.isTag {
			tags[fieldName] = fmt.Sprintf("%v", f)
		}

		if fieldData.isField {
			fields[fieldName] = f.Interface()
		}
	}

	if measurement == "" {
		measurement = dValue.Type().Name()
	}

	return
}

// Decode is used to process data returned by an InfluxDb query and uses reflection
// to transform it into an array of structs of type result.
//
// This function is used internally by the Query function.
func decode(influxResult []influxModels.Row, result interface{}) error {
	influxData := make([]map[string]interface{}, 0)

	for _, series := range influxResult {
		for _, v := range series.Values {
			r := make(map[string]interface{})
			for i, c := range series.Columns {
				if len(v) >= i+1 {
					r[c] = v[i]
				}
			}
			for tag, val := range series.Tags {
				r[tag] = val
			}
			r[InfluxMeasurement] = series.Name

			influxData = append(influxData, r)
		}
	}

	metadata := &mapstructure.Metadata{}
	config := &mapstructure.DecoderConfig{
		Metadata:         metadata,
		Result:           result,
		TagName:          "influx",
		WeaklyTypedInput: false,
		ZeroFields:       false,
		DecodeHook: func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
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

type influxFieldTagData struct {
	fieldName string
	isTag     bool
	isField   bool
}

func getInfluxFieldTagData(fieldName, structTag string) influxFieldTagData {
	fieldData := influxFieldTagData{fieldName: fieldName}
	parts := strings.Split(structTag, ",")
	fieldName, parts = parts[0], parts[1:]
	if fieldName != "" {
		fieldData.fieldName = fieldName
	}

	for _, part := range parts {
		switch part {
		case "tag":
			fieldData.isTag = true
		case "field":
			fieldData.isField = true
		}
	}

	if !fieldData.isField && !fieldData.isTag {
		fieldData.isField = true
	}

	return fieldData
}
