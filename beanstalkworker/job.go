package beanstalkworker

import "time"

// JobManager interface represents a way to handle a job's lifecycle.
type JobManager interface {
	Delete()
	Release()
	LogError(a ...interface{})
	LogInfo(a ...interface{})
	GetAge() time.Duration
	GetPriority() uint32
	GetReleases() uint32
	GetReserves() uint32
	GetTimeouts() uint32
	GetDelay() time.Duration
	GetTube() string
	SetReturnPriority(prio uint32)
	SetReturnDelay(delay time.Duration)
}
