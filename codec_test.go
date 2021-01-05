package influx

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	influxModels "github.com/influxdata/influxdb1-client/models"
)

func TestEncodeDataNotStruct(t *testing.T) {
	_, _, _, _, err := encode([]int{1, 2, 3}, nil)
	if err == nil {
		t.Error("Expected error")
	}
}

func TestEncodeSetsMesurment(t *testing.T) {
	type MyType struct {
		Val string `influx:"val"`
	}

	d := &MyType{"test-data"}
	_, _, _, measurement, err := encode(d, nil)
	if err != nil {
		t.Error("Error encoding: ", err)
	}

	if measurement != "MyType" {
		t.Errorf("%v != %v", measurement, "MyType")
	}
}

func TestEncodeUsesTimeField(t *testing.T) {
	type MyType struct {
		MyTimeField time.Time `influx:"my_time_field"`
		Val         string    `influx:"val"`
	}

	td, _ := time.Parse(time.RFC822, "27 Oct 78 15:04 PST")

	d := &MyType{td, "test-data"}
	tv, _, _, _, err := encode(d, &usingValue{"my_time_field", false})

	if tv != td {
		t.Error("Did not properly use the time field specified")
	}

	if err != nil {
		t.Error("Error encoding: ", err)
	}
}

func TestEncode(t *testing.T) {
	type MyType struct {
		InfluxMeasurement Measurement
		Time              time.Time `influx:"time"`
		TagValue          string    `influx:"tagValue,tag"`
		TagAndFieldValue  string    `influx:"tagAndFieldValue,tag,field"`
		IntValue          int       `influx:"intValue"`
		FloatValue        float64   `influx:"floatValue"`
		BoolValue         bool      `influx:"boolValue"`
		StringValue       string    `influx:"stringValue"`
		StructFieldName   string    `influx:""`
		IgnoredValue      string    `influx:"-"`
	}

	d := MyType{
		"test",
		time.Now(),
		"tag-value",
		"tag-and-field-value",
		10,
		10.5,
		true,
		"string",
		"struct-field",
		"ignored",
	}

	timeExp := d.Time

	tagsExp := map[string]string{
		"tagValue":         "tag-value",
		"tagAndFieldValue": "tag-and-field-value",
	}

	fieldsExp := map[string]interface{}{
		"tagAndFieldValue": d.TagAndFieldValue,
		"intValue":         d.IntValue,
		"floatValue":       d.FloatValue,
		"boolValue":        d.BoolValue,
		"stringValue":      d.StringValue,
		"StructFieldName":  d.StructFieldName,
	}

	tm, tags, fields, measurement, err := encode(d, nil)
	if err != nil {
		t.Error("Error encoding: ", err)
	}

	if measurement != d.InfluxMeasurement {
		t.Errorf("%v != %v", measurement, d.InfluxMeasurement)
	}

	if _, ok := fields["InfluxMeasurement"]; ok {
		t.Errorf("Found InfluxMeasurement in the fields!")
	}

	if !tm.Equal(timeExp) {
		t.Error("Time does not match")
	}

	if !reflect.DeepEqual(tags, tagsExp) {
		t.Error("tags not encoded correctly")
	}

	if !reflect.DeepEqual(fields, fieldsExp) {
		t.Error("fields not encoded correctly")
	}
}

func TestDecode(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"intValue",
			"floatValue",
			"boolValue",
			"stringValue",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{"tagValue": "tag-value"},
	}

	type DecodeType struct {
		TagValue     string  `influx:"tagValue,tag"`
		IntValue     int     `influx:"intValue"`
		FloatValue   float64 `influx:"floatValue"`
		BoolValue    bool    `influx:"boolValue"`
		StringValue  string  `influx:"stringValue"`
		IgnoredValue string  `influx:"-"`
	}

	var expected []DecodeType

	for i := 0; i < 10; i++ {
		v := DecodeType{
			"tag-value",
			i,
			float64(i),
			math.Mod(float64(i), 2) == 0,
			strconv.Itoa(i),
			"",
		}

		vI := []interface{}{
			v.IntValue,
			v.FloatValue,
			v.BoolValue,
			v.StringValue,
		}

		expected = append(expected, v)
		data.Values = append(data.Values, vI)

	}

	var decoded []DecodeType

	err := decode([]influxModels.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeMissingColumn(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"val1",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Val1 int `influx:"val1"`
		Val2 int `influx:"val2"`
	}

	expected := []DecodeType{{1, 0}}
	data.Values = append(data.Values, []interface{}{1})
	var decoded []DecodeType
	err := decode([]influxModels.Row{data}, &decoded)
	if err != nil {
		t.Error("UnExpected error decoding: ", data, &decoded)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeWrongType(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"val1",
			"val2",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"val2"`
	}

	expected := []DecodeType{{1, 2.0}}
	data.Values = append(data.Values, []interface{}{1.0, 2})
	var decoded []DecodeType
	err := decode([]influxModels.Row{data}, &decoded)
	if err != nil {
		t.Error("Unexpected error decoding: ", err, data, decoded)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right", expected, decoded)
	}
}

func TestDecodeTime(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"time",
			"value",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Time  time.Time `influx:"time"`
		Value float64   `influx:"value"`
	}

	timeS := "2018-06-14T21:47:11Z"
	ti, err := time.Parse(time.RFC3339, timeS)
	if err != nil {
		t.Error("error parsing expected time: ", err)
	}

	expected := []DecodeType{{ti, 2.0}}
	data.Values = append(data.Values, []interface{}{timeS, 2.0})
	var decoded []DecodeType
	err = decode([]influxModels.Row{data}, &decoded)

	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeJsonNumber(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"val1",
			"val2",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"val2"`
	}

	expected := []DecodeType{{1, 2.0}}
	data.Values = append(data.Values, []interface{}{json.Number("1"), json.Number("2.0")})
	decoded := []DecodeType{}
	err := decode([]influxModels.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeUnsedStructValue(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"val1",
			"val2",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Val1 int     `influx:"val1"`
		Val2 float64 `influx:"-"`
	}

	expected := []DecodeType{{1, 0}}
	data.Values = append(data.Values, []interface{}{1, 1.1})
	var decoded []DecodeType
	err := decode([]influxModels.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestDecodeMeasure(t *testing.T) {
	data := influxModels.Row{
		Name: "bla",
		Columns: []string{
			"val1",
			"val2",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		InfluxMeasurement Measurement
		Val1              int     `influx:"val1"`
		Val2              float64 `influx:"-"`
	}

	expected := []DecodeType{{"bla", 1, 0}}
	data.Values = append(data.Values, []interface{}{1, 1.1})
	var decoded []DecodeType
	err := decode([]influxModels.Row{data}, &decoded)

	if decoded[0].InfluxMeasurement != expected[0].InfluxMeasurement {
		t.Error("Decoded Wrong measure")
	}

	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded value is not right")
	}
}

func TestTag(t *testing.T) {
	data := []struct {
		fieldTag        string
		structFieldName string
		fieldName       string
		isTag           bool
		isField         bool
	}{
		{"", "Test", "Test", false, true},
		{"", "Test", "Test", false, true},
		{",tag", "Test", "Test", true, false},
		{",field,tag", "Test", "Test", true, true},
		{",tag,field", "Test", "Test", true, true},
		{",field", "Test", "Test", false, true},
		{"test", "Test", "test", false, true},
		{"test,tag", "Test", "test", true, false},
		{"test,field,tag", "Test", "test", true, true},
		{"test,tag,field", "Test", "test", true, true},
		{"test,field", "Test", "test", false, true},
	}

	for _, testData := range data {
		fieldData := getInfluxFieldTagData(testData.structFieldName, testData.fieldTag)
		if fieldData.fieldName != testData.fieldName {
			t.Errorf("%v != %v", fieldData.fieldName, testData.fieldName)
		}
		if fieldData.isField != testData.isField {
			t.Errorf("%v != %v", fieldData.isField, testData.isField)
		}
		if fieldData.isTag != testData.isTag {
			t.Errorf("%v != %v", fieldData.isTag, testData.isTag)
		}
	}
}
