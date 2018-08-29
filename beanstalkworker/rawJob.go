package beanstalkworker

import "time"
import "github.com/tomponline/beanstalk"
import "fmt"

// RawJob represents the raw job data that is returned by beanstalkd.
type RawJob struct {
	id          uint64
	err         error
	body        *[]byte
	conn        *beanstalk.Conn
	tube        string
	prio        uint32
	releases    uint32
	reserves    uint32
	timeouts    uint32
	delay       time.Duration
	age         time.Duration
	returnPrio  uint32
	returnDelay time.Duration
	log         *Logger
}

// Initialise a new empty RawJob with a custom logger
// Useful for testing methods that log messages on the job
func NewEmptyJob(cl CustomLogger) *RawJob {
	logger := &Logger{
		Info:   cl.Info,
		Infof:  cl.Infof,
		Error:  cl.Error,
		Errorf: cl.Errorf,
	}

	return &RawJob{
		log: logger,
	}
}

// Delete function deletes the job from the queue.
func (job *RawJob) Delete() {
	if err := job.conn.Delete(job.id); err != nil {
		job.log.Error("Could not delete job: " + err.Error())
	}
}

// Release function releases the job from the queue.
func (job *RawJob) Release() {
	if err := job.conn.Release(job.id, job.returnPrio, job.returnDelay); err != nil {
		job.log.Error("Could not release job: " + err.Error())
	}
}

// Bury function buries the job from the queue.
func (job *RawJob) Bury() {
	if err := job.conn.Bury(job.id, job.returnPrio); err != nil {
		job.log.Error("Could not bury job: " + err.Error())
	}
}

// SetReturnPriority sets the return priority to use if a job is released or buried.
func (job *RawJob) SetReturnPriority(prio uint32) {
	job.returnPrio = prio
}

// SetReturnDelay sets the return delay to use if a job is released back to queue.
func (job *RawJob) SetReturnDelay(delay time.Duration) {
	job.returnDelay = delay
}

// GetAge gets the age of the job from the job stats.
func (job *RawJob) GetAge() time.Duration {
	return job.age
}

// GetDelay gets the delay of the job from the job stats.
func (job *RawJob) GetDelay() time.Duration {
	return job.delay
}

// GetPriority gets the priority of the job.
func (job *RawJob) GetPriority() uint32 {
	return job.prio
}

// GetReleases gets the count of release of the job.
func (job *RawJob) GetReleases() uint32 {
	return job.releases
}

// GetReserves gets the count of reserves of the job.
func (job *RawJob) GetReserves() uint32 {
	return job.reserves
}

// GetTimeouts gets the count of timeouts of the job.
func (job *RawJob) GetTimeouts() uint32 {
	return job.timeouts
}

// GetTube returns the tube name we got this job from.
func (job *RawJob) GetTube() string {
	return job.tube
}

// GetConn returns the beanstalk connection used to receive the job.
func (job *RawJob) GetConn() *beanstalk.Conn {
	return job.conn
}

// LogError function logs an error message regarding the job.
func (job *RawJob) LogError(a ...interface{}) {
	job.log.Error("Tube: ", job.tube, ", Job: ", job.id, ": Error: ", fmt.Sprint(a...))
}

// LogInfo function logs an info message regarding the job.
func (job *RawJob) LogInfo(a ...interface{}) {
	job.log.Info("Tube: ", job.tube, ", Job: ", job.id, ": ", fmt.Sprint(a...))
}
