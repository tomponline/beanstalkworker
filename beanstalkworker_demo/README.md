# beanstalkworker_demo
An example program to demonstrate beanstalkworker package usage.

To generate jobs into the beanstalkd queue for this demo use beanstool command:

```
 go get -u github.com/src-d/beanstool
 beanstool put -t job1 -b '{"someField":"hello world"}' --ttr=15s --priority=200
```
