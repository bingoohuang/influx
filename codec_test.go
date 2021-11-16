package influx_test

import (
	"encoding/json"
	"github.com/bingoohuang/influx"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb1-client/models"
)

func TestEncodeDataNotStruct(t *testing.T) {
	_, err := influx.Encode([]int{1, 2, 3}, "")
	if err == nil {
		t.Error("Expected error")
	}
}

func TestEncodeSetsMesurment(t *testing.T) {
	type MyType struct {
		Val string `influx:"val"`
	}

	d := &MyType{"test-data"}
	p, err := influx.Encode(d, "")
	if err != nil {
		t.Error("Error encoding: ", err)
	}

	if p.Measurement != "MyType" {
		t.Errorf("%v != %v", p.Measurement, "MyType")
	}
}

func TestEncodeUsesTimeField(t *testing.T) {
	type MyType struct {
		MyTimeField time.Time `influx:"my_time_field"`
		Val         string    `influx:"val"`
	}

	td, _ := time.Parse(time.RFC822, "27 Oct 78 15:04 PST")

	d := &MyType{td, "test-data"}
	p, err := influx.Encode(d, "my_time_field")
	if err != nil {
		t.Error("Error encoding: ", err)
	}

	if p.Time != td {
		t.Error("Did not properly use the time field specified")
	}
}

func TestEncode(t *testing.T) {
	type MyType struct {
		InfluxMeasurement string
		Time              time.Time `influx:"time"`
		TagValue          string    `influx:"tagValue,tag"`
		TagAndFieldValue  string    `influx:"tagAndFieldValue,tag,field"`
		IntValue          int       `influx:"intValue"`
		FloatValue        float64   `influx:"floatValue"`
		BoolValue         bool      `influx:"boolValue"`
		StringValue       string    `influx:"stringValue"`
		StructFieldName   string
		IgnoredValue      string `influx:"-"`
	}

	d := MyType{
		"test",
		time.Now(),
		"tag-Value",
		"tag-and-field-Value",
		10,
		10.5,
		true,
		"string",
		"struct-field",
		"ignored",
	}

	timeExp := d.Time

	tagsExp := map[string]string{
		"tagValue":         "tag-Value",
		"tagAndFieldValue": "tag-and-field-Value",
	}

	fieldsExp := map[string]interface{}{
		"tagAndFieldValue": d.TagAndFieldValue,
		"intValue":         d.IntValue,
		"floatValue":       d.FloatValue,
		"boolValue":        d.BoolValue,
		"stringValue":      d.StringValue,
		"StructFieldName":  d.StructFieldName,
	}

	p, err := influx.Encode(d, "")
	if err != nil {
		t.Error("Error encoding: ", err)
	}

	if p.Measurement != d.InfluxMeasurement {
		t.Errorf("%v != %v", p.Measurement, d.InfluxMeasurement)
	}

	if _, ok := p.Fields[influx.InfluxMeasurement]; ok {
		t.Errorf("Found InfluxMeasurement in the fields!")
	}

	if !p.Time.Equal(timeExp) {
		t.Error("Time does not match")
	}

	if !reflect.DeepEqual(p.Tags, tagsExp) {
		t.Error("tags not encoded correctly")
	}

	if !reflect.DeepEqual(p.Fields, fieldsExp) {
		t.Error("fields not encoded correctly")
	}
}

func TestDecode(t *testing.T) {
	data := models.Row{
		Name: "bla",
		Columns: []string{
			"intValue",
			"floatValue",
			"boolValue",
			"stringValue",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{"tagValue": "tag-Value"},
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
			"tag-Value",
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

	err := influx.Decode([]models.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
	}
}

func TestDecodeMissingColumn(t *testing.T) {
	data := models.Row{
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
	err := influx.Decode([]models.Row{data}, &decoded)
	if err != nil {
		t.Error("UnExpected error decoding: ", data, &decoded)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
	}
}

func TestDecodeWrongType(t *testing.T) {
	data := models.Row{
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
	err := influx.Decode([]models.Row{data}, &decoded)
	if err != nil {
		t.Error("Unexpected error decoding: ", err, data, decoded)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right", expected, decoded)
	}
}

func TestDecodeTime(t *testing.T) {
	data := models.Row{
		Name: "bla",
		Columns: []string{
			"time",
			"Value",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		Time  time.Time `influx:"time"`
		Value float64   `influx:"Value"`
	}

	timeS := "2018-06-14T21:47:11Z"
	ti, err := time.Parse(time.RFC3339, timeS)
	if err != nil {
		t.Error("error parsing expected time: ", err)
	}

	expected := []DecodeType{{ti, 2.0}}
	data.Values = append(data.Values, []interface{}{timeS, 2.0})
	var decoded []DecodeType
	err = influx.Decode([]models.Row{data}, &decoded)

	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
	}
}

func TestDecodeJsonNumber(t *testing.T) {
	data := models.Row{
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
	var decoded []DecodeType
	err := influx.Decode([]models.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
	}
}

func TestDecodeUnsedStructValue(t *testing.T) {
	data := models.Row{
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
	err := influx.Decode([]models.Row{data}, &decoded)
	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
	}
}

func TestDecodeMeasure(t *testing.T) {
	data := models.Row{
		Name: "bla",
		Columns: []string{
			"val1",
			"val2",
		},
		Values: make([][]interface{}, 0),
		Tags:   map[string]string{},
	}

	type DecodeType struct {
		InfluxMeasurement string
		Val1              int     `influx:"val1"`
		Val2              float64 `influx:"-"`
	}

	expected := []DecodeType{{"bla", 1, 0}}
	data.Values = append(data.Values, []interface{}{1, 1.1})
	var decoded []DecodeType
	err := influx.Decode([]models.Row{data}, &decoded)

	if decoded[0].InfluxMeasurement != expected[0].InfluxMeasurement {
		t.Error("Decoded Wrong measure")
	}

	if err != nil {
		t.Error("Error decoding: ", err)
	}

	if !reflect.DeepEqual(expected, decoded) {
		t.Error("decoded Value is not right")
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
		{",tag", "Test", "Test", true, false},
		{",field,tag", "Test", "Test", true, true},
		{",tag,field", "Test", "Test", true, true},
		{",field", "Test", "Test", false, true},
		{"test", "Test", "test", false, true},
		{"test,tag", "Test", "test", true, false},
		{"test,field,tag", "Test", "test", true, true},
		{"test,tag,field", "Test", "test", true, true},
		{"-,tag,field", "Test", "-", false, false},
		{"test,field", "Test", "test", false, true},
		{"-", "Test", "-", false, false},
	}

	for _, testData := range data {
		fieldData := influx.ParseInfluxTag(testData.structFieldName, testData.fieldTag)
		if fieldData.FieldName != testData.fieldName {
			t.Errorf("%v != %v", fieldData.FieldName, testData.fieldName)
		}
		if fieldData.IsField != testData.isField {
			t.Errorf("%v != %v", fieldData.IsField, testData.isField)
		}
		if fieldData.IsTag != testData.isTag {
			t.Errorf("%v != %v", fieldData.IsTag, testData.isTag)
		}
	}
}
