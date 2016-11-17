package beanstalkworker

import "strconv"
import "time"
import "github.com/kr/beanstalk"
import "fmt"
import "log"

// PrioLow is the priority that jobs are released/buried with.
const PrioLow = 1025

// RawJob represents the raw job data that is returned by beanstalkd.
type RawJob struct {
	id   uint64
	err  error
	body *[]byte
	conn *beanstalk.Conn
	tube string
}

// Delete function deletes the job from the queue.
func (job *RawJob) Delete() {
	if err := job.conn.Delete(job.id); err != nil {
		job.LogError("Could not delete job: " + err.Error())
	}
}

// Release function releases the job from the queue.
func (job *RawJob) Release() {
	if err := job.conn.Release(job.id, PrioLow, 30*time.Second); err != nil {
		job.LogError("Could not release job: " + err.Error())
	}
}

// Bury function buries the job from the queue.
func (job *RawJob) Bury() {
	if err := job.conn.Bury(job.id, PrioLow); err != nil {
		job.LogError("Could not bury job: " + err.Error())
	}
}

// GetAge gets the age of the job (in seconds) from the beanstalkd server.
func (job *RawJob) GetAge() (int, error) {
	stats, err := job.conn.StatsJob(job.id)
	if err != nil {
		return 0, err
	}

	age, err := strconv.Atoi(stats["age"])
	if err != nil {
		return 0, err
	}

	return age, nil
}

// GetTube returns the tube name we got this job from.
func (job *RawJob) GetTube() string {
	return job.tube
}

// LogError function logs an error messagge regarding the job.
func (job *RawJob) LogError(a ...interface{}) {
	log.Print("Tube: ", job.tube, ", Job: ", job.id, ": Error: ", fmt.Sprint(a...))
}

// LogInfo function logs an info messagge regarding the job.
func (job *RawJob) LogInfo(a ...interface{}) {
	log.Print("Tube: ", job.tube, ", Job: ", job.id, ": ", fmt.Sprint(a...))
}
