package influx_test

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/bingoohuang/influx"
	"github.com/go-playground/assert/v2"
	client "github.com/influxdata/influxdb1-client/v2"
)

func TestExample20(t *testing.T) {
	cli, _ := influx.New(influx.WithAddr("http://localhost:8086"))
	var m map[string]string
	// INSERT weather,location=us-midwest temperature=82,humidity=71 1465839830100400200
	tags := make(map[string][]string)
	cli.UseDB("king").DecodeQuery(`select * from king.autogen.weather`, &m, influx.WithTagsReturn(&tags, 2))
	cli.UseDB("king").DecodeQuery(`select * from king.autogen.weather`, &m, influx.WithTagsReturn(&tags, 2))

	fmt.Println(m)
	fmt.Println(tags)
	// Output:
	//	map[InfluxMeasurement:weather humidity:71 location:us-midwest temperature:82 time:2016-06-13T17:43:50.1004002Z]
	//	map[location:[us-midwest]]
}

func TestExample21(t *testing.T) {
	cli, _ := influx.New(influx.WithAddr("http://192.168.127.23:10014"))
	var m map[string]string
	tags := make(map[string][]string)
	cli.UseDB("metrics").DecodeQuery(`select * from metrics.autogen.QPS_dsvsServer order by time desc limit 100`, &m, influx.WithTagsReturn(&tags, 0))

	fmt.Println(m)
	fmt.Println(tags)
}

func TestExample(t *testing.T) {
	cli, _ := influx.New(influx.WithAddr("http://localhost:8086"))
	var m map[string]string
	cli.DecodeQuery(`select * from telegraf.oneweek.cpu where time > now() - 5m order by time desc limit 1`, &m)

	fmt.Println(m)
	// Output:
	// map[InfluxMeasurement:cpu cpu:cpu7 host:tencent-beta01 ip:192.168.106.3 ips:192.168.106.3 time:2021-12-09T04:31:22Z time_active:62547.40000000037 time_guest:0 time_guest_nice:0 time_idle:6827183.36 time_iowait:216.63 time_irq:14.15 time_nice:5.92 time_softirq:1013.13 time_steal:0 time_system:28451.05 time_user:32846.52 usage_active:0.2004008064468124 usage_guest:0 usage_guest_nice:0 usage_idle:99.79959919355319 usage_iowait:0 usage_irq:0 usage_nice:0 usage_softirq:0 usage_steal:0 usage_system:0.10020040078107574 usage_user:0.10020040074462304]
}

func TestInflux(t *testing.T) {
	const db = "demo"

	c, err := influx.New(influx.WithAddr("http://localhost:8086"))
	if err != nil {
		return
	}

	// Create test database if it doesn't already exist
	cq := client.NewQuery("DROP DATABASE "+db, "", "")
	c.Query(cq)

	// Create test database if it doesn't already exist
	cq = client.NewQuery("CREATE DATABASE "+db, "", "")
	res, err := c.Query(cq)
	if err != nil {
		panic(err)
	}

	if res.Error() != nil {
		panic(res.Error())
	}
	log.Print("db initialized")

	// write sample data to database
	samples := generateSampleData()
	c = c.UseDB(db)
	for _, p := range samples {
		if err := c.WritePoint(p); err != nil {
			log.Fatal("Error writing point: ", err)
		}
	}

	var samplesRead []envSample

	if err = c.UseDB(db).DecodeQuery(`SELECT * FROM test ORDER BY time`, &samplesRead); err != nil {
		log.Fatal("Query error: ", err)
	}

	s1, _ := json.Marshal(samples)
	s2, _ := json.Marshal(samplesRead)
	assert.Equal(t, s1, s2)
}

type envSample struct {
	_           string `influx:",measurement:test"`
	Time        time.Time
	Location    string `influx:",tag"`
	Temperature float64
	Humidity    float64
	ID          string `influx:"-"`
}

func generateSampleData() []envSample {
	ret := make([]envSample, 10)

	for i := range ret {
		ret[i] = envSample{
			Time:        time.Now().Add(time.Duration(i) * time.Second),
			Location:    "Rm 243",
			Temperature: 70 + float64(i),
			Humidity:    60 - float64(i),
			ID:          "",
		}
	}

	return ret
}

func ExampleClient_WritePoint() {
	c, _ := influx.New(influx.WithAddr("http://localhost:8086"))

	type EnvSample struct {
		Time        time.Time
		Location    string `influx:",tag"`
		Temperature float64
		Humidity    float64
		ID          string `influx:"-"`
	}

	s := EnvSample{
		Time:        time.Now(),
		Location:    "Rm 243",
		Temperature: 70.0,
		Humidity:    60.0,
		ID:          "12432as32",
	}

	_ = c.UseDB("myDb").WritePoint(s)
}

func ExampleClient_Query() {
	c, _ := influx.New(influx.WithAddr("http://localhost:8086"))

	type EnvSample struct {
		Time        time.Time
		Location    string `influx:",tag"`
		Temperature float64
		Humidity    float64
		ID          string `influx:"-"`
	}

	var samplesRead []EnvSample

	q := `SELECT * FROM test ORDER BY time DESC LIMIT 10`
	_ = c.UseDB("myDb").DecodeQuery(q, &samplesRead)

	// samplesRead is now populated with data from InfluxDb
}
