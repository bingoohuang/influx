# InfluxDb Helper Library

> easily write and query InfluxDb on Go structs

This library allows you to encode/decode InfluxDb data to/from Go structs -- similar to JSON and MongoDb using Go struct
field tags.  This repo is originally from [cbrake/influxdbhelper](https://github.com/cbrake/influxdbhelper)

## Install

```
go get github.com/bingoohuang/influx
```

## Example

See a working example [here](cmd/influxdemo/main.go)

## Details

There are several advantages decoding and encoding data directly from Go Structs:

1. The database bschema is documented by the Go type definition. This helps ensure data is written consistently to the
   database. When all your data is clearly defined in Go structs, it is much more obvious how to organize it, what goes
   in what measurement, when to create a new measurement, etc. When writing straight tags/values, it is much easier to
   create a disorganized mess.
1. All the code for decoding and encoding the various data types supported by InfluxDb are handled in one place, rather
   than repeating this logic over and over for every Query.
1. Likewise, code for handling arrays (translating Go array to InfluxDb fields like temp0, temp1, temp2, ...) can be in
   one place.
1. Reading and Writing data is much simpler and requires way less code.

Using Go reflection to automate data decode may be slightly slower than custom decode logic for every query, but it
seems the time to decode the data will relatively fast compared to the time to run a InfluxDb query, so may be
negligible (this is an assumption at this point and has not been proven).

The codec_test.go file contains a number of tests that illustrate the conversion from influx JSON to Go struct values.

## Acknowledgments

The [mapstructure](https://github.com/mitchellh/mapstructure) library provided a very useful reference for learning how
to use the Go reflect functionality.

## Status

Todo:

- [x] handle larger query datasets (multiple series, etc)
- [x] add write capability (directly write Go structs into influxdb)
- [ ] get working with influxdb 1.7 client
- [ ] see if still applicable for influxdb 2.x
- [ ] decode/encode val0, val1, val2 fields in influx to Go array
- [ ] use Go struct field tags to help build SELECT statement
- [ ] optimize query for performance (pre-allocate slices, etc)

Review/Pull requests welcome!
