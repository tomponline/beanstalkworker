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
	handler.LogInfo("Starting job with commonVar ", handler.commonVar)
	handler.LogInfo("Job Data unserialised: ", jobData)
	time.Sleep(2 * time.Second) //Simulate worker process
	handler.Release()           //Pretend job process failed
}
