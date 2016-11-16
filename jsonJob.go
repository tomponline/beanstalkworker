package beanstalkworker

import "fmt"

// JSONJob represents a decoded raw job that contains JSON fields that are
// compatible with a map[string]string structure.
type JSONJob struct {
	RawJob
	fields   map[string]string
	typeName string
}

// GetField retrieves a particular field from the job's data.
func (job *JSONJob) GetField(field string) string {
	return job.fields[field]
}

// LogError logs an error messagge regarding the job.
func (job *JSONJob) LogError(a ...interface{}) {
	job.RawJob.LogError("Type: ", job.typeName, ": ", fmt.Sprint(a...))
}

// LogInfo logs an info messagge regarding the job.
func (job *JSONJob) LogInfo(a ...interface{}) {
	job.RawJob.LogInfo("Type: ", job.typeName, ": ", fmt.Sprint(a...))
}
