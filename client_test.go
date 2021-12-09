package influx_test

import (
	"fmt"
	"github.com/bingoohuang/influx"
	client "github.com/influxdata/influxdb1-client/v2"
	"log"
	"time"
)

func ExampleInfluxLast() {
	cli, _ := influx.New(influx.WithAddr("http://beta.isignet.cn:10014"))
	var m map[string]string
	cli.DecodeQuery(`select * from telegraf.oneweek.cpu where time > now() - 5m order by time desc limit 1`, &m)

	fmt.Println(m)
	// Output:
	// map[InfluxMeasurement:cpu cpu:cpu7 host:tencent-beta01 ip:192.168.106.3 ips:192.168.106.3 time:2021-12-09T04:31:22Z time_active:62547.40000000037 time_guest:0 time_guest_nice:0 time_idle:6827183.36 time_iowait:216.63 time_irq:14.15 time_nice:5.92 time_softirq:1013.13 time_steal:0 time_system:28451.05 time_user:32846.52 usage_active:0.2004008064468124 usage_guest:0 usage_guest_nice:0 usage_idle:99.79959919355319 usage_iowait:0 usage_irq:0 usage_nice:0 usage_softirq:0 usage_steal:0 usage_system:0.10020040078107574 usage_user:0.10020040074462304]
}
func ExampleInflux() {
	// Static connection configuration
	const influxURL = "http://localhost:8086"
	const db = "demo"

	c, err := influx.New(influx.WithAddr(influxURL))
	if err != nil {
		return
	}
	// Create test database if it doesn't already exist
	cq := client.NewQuery("CREATE DATABASE "+db, "", "")
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
		err := c.WritePoint(p)
		if err != nil {
			log.Fatal("Error writing point: ", err)
		}
	}

	// query data from db
	var samplesRead []envSampleRead

	q := `SELECT * FROM test ORDER BY time DESC LIMIT 10`
	err = c.UseDB(db).DecodeQuery(q, &samplesRead)
	if err != nil {
		log.Fatal("Query error: ", err)
	}

	log.Printf("Samples read: %+v", samplesRead)

	ShowMeasure(c)

	// Output:
}

func ShowMeasure(c *influx.Cli) {
	type Measurement struct {
		Name string `influx:"name"`
	}

	var measurements []Measurement

	err := c.UseDB("mydb").DecodeQuery(`show MEASUREMENTS WITH MEASUREMENT =~ /cpu_/`, &measurements)
	if err != nil {
		log.Printf("error: %v", err)
	}

	// measurements read: [{Name:cpu_load_short}]
	log.Printf("measurements read: %+v", measurements)

	type Sum struct {
		Sum float64 `influx:"sum"`
	}

	var sum []Sum

	err = c.UseDB("mydb").DecodeQuery(`select sum(Value) from cpu_load_short`, &sum)
	if err != nil {
		log.Printf("error: %v", err)
	}
	// sum read: [{Sum:0.64}]
	log.Printf("sum read: %+v", sum)

	type Cpu struct {
		Time   time.Time `influx:"time"`
		Host   string    `influx:"host"`
		Region string    `influx:"region"`
		Value  float64   `influx:"Value"`
	}

	var cpus []Cpu

	err = c.UseDB("mydb").DecodeQuery(`select * from cpu_load_short`, &cpus)
	if err != nil {
		log.Printf("error: %v", err)
	}
	//  cpus read: [{Time:2015-06-11 20:46:02 +0000 UTC Host:server01 Region:us-west Value:0.64}]
	log.Printf("cpus read: %+v", cpus)
}

type envSample struct {
	InfluxMeasurement string
	Time              time.Time `influx:"time"`
	Location          string    `influx:"location,tag"`
	Temperature       float64   `influx:"temperature"`
	Humidity          float64   `influx:"humidity"`
	ID                string    `influx:"-"`
}

// we populate a few more fields when reading back
// date to verify unused fields are handled correctly
type envSampleRead struct {
	InfluxMeasurement string
	Time              time.Time `influx:"time"`
	Location          string    `influx:"location,tag"`
	City              string    `influx:"city,tag,field"`
	Temperature       float64   `influx:"temperature"`
	Humidity          float64   `influx:"humidity"`
	Cycles            float64   `influx:"cycles"`
	ID                string    `influx:"-"`
}

func generateSampleData() []envSample {
	ret := make([]envSample, 10)

	for i := range ret {
		ret[i] = envSample{
			InfluxMeasurement: "test",
			Time:              time.Now(),
			Location:          "Rm 243",
			Temperature:       70 + float64(i),
			Humidity:          60 - float64(i),
			ID:                "12432as32",
		}
	}

	return ret
}

func ExampleClient_WritePoint() {
	c, _ := influx.New(influx.WithAddr("http://localhost:8086"))

	type EnvSample struct {
		Time        time.Time `influx:"time"`
		Location    string    `influx:"location,tag"`
		Temperature float64   `influx:"temperature"`
		Humidity    float64   `influx:"humidity"`
		ID          string    `influx:"-"`
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
		Time        time.Time `influx:"time"`
		Location    string    `influx:"location,tag"`
		Temperature float64   `influx:"temperature"`
		Humidity    float64   `influx:"humidity"`
		ID          string    `influx:"-"`
	}

	var samplesRead []EnvSample

	q := `SELECT * FROM test ORDER BY time DESC LIMIT 10`
	_ = c.UseDB("myDb").DecodeQuery(q, &samplesRead)

	// samplesRead is now populated with data from InfluxDb
}
