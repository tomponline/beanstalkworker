package main

import "github.com/tomponline/beanstalkworker/beanstalkworker"
import "time"
import "fmt"

// Job1Handler contains the business logic to handle the Job1 type jobs.
type Job1Handler struct {
	beanstalkworker.JobManager
	commonVar string
}

// Job1Data is a struct that represents the Job1 data that arrives from the queue.
type Job1Data struct {
	SomeField      string `json:"someField"`
	SomeOtherField int    `json:"someOtherField"`
}

// LogError example of overriding a function provided in beanstalkworker.JobManager
// and calling the underlying function in order to add context.
func (handler *Job1Handler) LogError(a ...interface{}) {
	handler.JobManager.LogError("Job1 error: ", fmt.Sprint(a...))
}

// Run is executed by the beanstalk worker when a Job1 type job is received.
func (handler *Job1Handler) Run(jobData Job1Data) {
	handler.LogInfo("Starting job with commonVar value: ", handler.commonVar)
	handler.LogInfo("Job Data received: ", jobData)
	handler.LogInfo("Job Priority: ", handler.GetPriority())
	handler.LogInfo("Job Releases: ", handler.GetReleases())
	handler.LogInfo("Job Age: ", handler.GetAge())
	handler.LogInfo("Job Tube: ", handler.GetTube())
	time.Sleep(2 * time.Second)             //Simulate job processing time
	handler.SetReturnDelay(5 * time.Second) //Optional return delay (defaults to 30s)
	handler.SetReturnPriority(5)            //Optional return priority (defaults to current priority)

	if handler.GetReleases() >= 3 {
		handler.Delete()
		handler.LogError("Deleting job as too many releases")
		return
	}

	handler.LogInfo("Releasing job to be retried...")
	handler.Release() //Pretend job process failed and needs retrying
}
