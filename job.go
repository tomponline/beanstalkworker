package beanstalkworker

import "time"

// JobManager interface represents a way to handle a job's lifecycle.
type JobManager interface {
	Delete()
	Release()
	LogError(a ...interface{})
	LogInfo(a ...interface{})
	GetAge() (time.Duration)
	GetPriority() uint32
	GetTube() string
	SetReturnPriority(prio uint32)
	SetReturnDelay(delay time.Duration)
}
