package beanstalkworker

// JobManager interface represents a way to handle a job's lifecycle.
type JobManager interface {
	Delete()
	Release()
	LogError(a ...interface{})
	LogInfo(a ...interface{})
	GetAge() (int, error)
	GetTube() string
}
