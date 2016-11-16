# beanstalkworker
A helper library for creating beanstalkd consumer processes.

## Aims

* To provide a generic way for consuming beanstalkd jobs without all of the boiler plate code
* To provide an easy way to spin up concurrent worker Go routines
* To use Go's interfaces to make unit testing your workers easy

## Usage

Please see https://github.com/tomponline/beanstalkworker_demo for examples of how to use this library.

## See also

This library is	a wrapper around the low-level Beanstalkd client written in Go:

https://github.com/kr/beanstalk

This client talks to Beanstalkd queue server:

http://kr.github.io/beanstalkd/
