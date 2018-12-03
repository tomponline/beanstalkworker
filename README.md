[![GoDoc](https://godoc.org/github.com/tomponline/beanstalkworker?status.svg)](https://godoc.org/github.com/tomponline/beanstalkworker)
# beanstalkworker
A helper library for creating beanstalkd consumer processes.

## Usage

```go get -u github.com/tomponline/beanstalkworker```

## Docs/Examples

Please see Go Docs for usage and examples:

https://godoc.org/github.com/tomponline/beanstalkworker

## Aims

* To provide a generic way for consuming beanstalkd jobs without all of the boiler plate code
* To provide an easy way to spin up concurrent worker Go routines
* To use Go's interfaces to make unit testing your workers easy

## Details

The library is broken down into the following components:

* JobManager interface - represents a way to handle a job's lifecycle.
* RawJob - an implementation of JobManager for managing a Raw job's life cycle.
* Worker - an implementation of a beanstalkd client process that consumes raw jobs from one or more tubes. It will automatically reconnect to beanstalkd server if it loses the connection.

## See also

This library is	a wrapper around the low-level Beanstalkd client written in Go:

https://github.com/beanstalkd/go-beanstalk

This client talks to Beanstalkd queue server:

https://beanstalkd.github.io/
