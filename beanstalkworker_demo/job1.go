package main

import "github.com/tomponline/beanstalkworker/beanstalkworker"
import "time"

type Job1Handler struct {
	beanstalkworker.JobManager
	commonVar string
}

type Job1Data struct {
	SomeField      string `json:"someField"`
	SomeOtherField int    `json:"someOtherField"`
}

func (handler *Job1Handler) Run(jobData Job1Data) {
	handler.LogInfo("Starting job with commonVar value: ", handler.commonVar)
	handler.LogInfo("Job Data recieved: ", jobData)
	handler.LogInfo("Job Priority: ", handler.GetPriority())
	handler.LogInfo("Job Releases: ", handler.GetReleases())
	handler.LogInfo("Job Age: ", handler.GetAge())
	handler.LogInfo("Job Tube: ", handler.GetTube())
	time.Sleep(2 * time.Second)             //Simulate job processing time
	handler.SetReturnDelay(5 * time.Second) //Optional return delay (defaults to 30s)
	handler.SetReturnPriority(5)            //Optional return priority (defaults to current priority)

	if handler.GetReleases() > 5 {
		handler.Delete()
		handler.LogInfo("Deleting job as too many releases")
		return
	}

	handler.LogInfo("Releasing job to be retried...")
	handler.Release() //Pretend job process failed and needs retrying
}
