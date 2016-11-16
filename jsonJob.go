package beanstalkworker

import "fmt"

type JSONJob struct {
	RawJob
	fields   map[string]string
	typeName string
}

func (job *JSONJob) GetField(field string) string {
	return job.fields[field]
}

// logError function logs an error messagge regarding the job referenced by Job.
func (job *JSONJob) LogError(a ...interface{}) {
	job.RawJob.LogError("Type: ", job.typeName, ": ", fmt.Sprint(a...))
}

// logInfo function logs an info messagge regarding the job referenced by Job.
func (job *JSONJob) LogInfo(a ...interface{}) {
	job.RawJob.LogInfo("Type: ", job.typeName, ": ", fmt.Sprint(a...))
}
