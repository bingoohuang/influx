package main

import (
	"log"
	"time"

	"github.com/bingoohuang/influx"
	client "github.com/influxdata/influxdb1-client/v2"
)

const (
	// Static connection configuration
	influxURL = "http://localhost:8086"
	db        = "demo"
)

var c *influx.Cli

// Init initializes the database connection
func Init() (err error) {
	c, err = influx.NewClient(influxURL, "", "", "ns")
	if err != nil {
		return
	}
	// Create test database if it doesn't already exist
	q := client.NewQuery("CREATE DATABASE "+db, "", "")
	res, err := c.Query(q)
	if err != nil {
		return err
	}
	if res.Error() != nil {
		return res.Error()
	}
	log.Print("dbhelper db initialized")
	return nil
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

func main() {
	err := Init()
	if err != nil {
		log.Fatal("Failed to initialize db")
	}

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

	ShowMeasure()
}

func ShowMeasure() {
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

	err = c.UseDB("mydb").DecodeQuery(`select sum(value) from cpu_load_short`, &sum)
	if err != nil {
		log.Printf("error: %v", err)
	}
	// sum read: [{Sum:0.64}]
	log.Printf("sum read: %+v", sum)

	type Cpu struct {
		Time   time.Time `influx:"time"`
		Host   string    `influx:"host"`
		Region string    `influx:"region"`
		Value  float64   `influx:"value"`
	}

	var cpus []Cpu

	err = c.UseDB("mydb").DecodeQuery(`select * from cpu_load_short`, &cpus)
	if err != nil {
		log.Printf("error: %v", err)
	}
	//  cpus read: [{Time:2015-06-11 20:46:02 +0000 UTC Host:server01 Region:us-west Value:0.64}]
	log.Printf("cpus read: %+v", cpus)
}
