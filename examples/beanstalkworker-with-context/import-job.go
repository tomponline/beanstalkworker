package main

import (
	"encoding/json"
	"fmt"

	"github.com/tomponline/beanstalkworker"
)

// ImportJobData represents the job data that arrives from the import-jobs queue
// we'll use map[string]string to keep the transport encoding simple for interoperability
type ImportJobData map[string]string

// ImportJobHandler contains all the helpers needed to process a job
type ImportJobHandler struct {
	// Embed the job manager
	beanstalkworker.JobManager

	jobData ImportJobData
}

// Run is executed for every job
func (job *ImportJobHandler) Run() {
	// Validate our job data, we can't process invalid jobs
	if err := job.validateJobData(); err != nil {
		// Attempt to re-marshal our job data and log it
		jobStr, mrshErr := json.Marshal(job.jobData)
		if mrshErr != nil {
			// We shouldn't ever have a problem marshalling a map[string]string
			// but never say never
			job.LogError("Could not re-marshal job data: ", mrshErr)
		}

		// Log the job data then delete the job, we will never be able to process this job
		// as it requires manual intervention
		job.LogError("Job data is invalid: ", err, ", deleting job: ", string(jobStr))
		job.Delete()

		return
	}

	job.LogInfo("Got an import job of type ", job.jobData["type"])

	// Delete the job when we have finished processing it
	job.Delete()
}

// validates job data before processing to ensure required job data is present
func (job *ImportJobHandler) validateJobData() error {
	// Required fields on our job
	requiredFields := []string{"type"}

	// Check all required fields have a value set
	for _, field := range requiredFields {
		value, exists := job.jobData[field]
		if !exists {
			return fmt.Errorf("Field %s not found in job data", field)
		}

		if value == "" {
			return fmt.Errorf("Field %s should not be empty", field)
		}
	}

	return nil
}
