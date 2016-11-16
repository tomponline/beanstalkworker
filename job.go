package beanstalkworker

// JobManager interface represents a way to handle a job's lifecycle.
type JobManager interface {
	Delete()
	Release()
	LogError(a ...interface{})
	LogInfo(a ...interface{})
}

// JobAccessor interface represents a way to access a job's decoded data fields.
type JobAccessor interface {
	JobManager
	GetField(field string) string
}
