package beanstalkworker

type Job interface {
	Delete()
	Release()
	LogError(a ...interface{})
	LogInfo(a ...interface{})
}

type JobDecoded interface {
	Job
	GetField(field string) string
}
