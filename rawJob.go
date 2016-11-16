package beanstalkworker

import "strconv"
import "time"
import "github.com/kr/beanstalk"
import "fmt"
import "log"

const beanstalkdPrioHigh = 1023
const beanstalkdPrioNorm = 1024
const beanstalkdPrioLow = 1025
const beanstalkdDefaultTtr = 60 //Default time to run

type RawJob struct {
	id   uint64
	err  error
	body *[]byte
	conn *beanstalk.Conn
}

// deleteJob function deletes the job from the queue that is referenced by Job.
func (job *RawJob) Delete() {
	if err := job.conn.Delete(job.id); err != nil {
		job.LogError("Could not delete job: " + err.Error())
	}
}

// releaseJob function releases the job from the queue that is referenced by Job.
func (job *RawJob) Release() {
	if err := job.conn.Release(job.id, beanstalkdPrioLow, 30*time.Second); err != nil {
		job.LogError("Could not release job: " + err.Error())
	}
}

// buryJob function buries the job from the queue that is referenced by Job.
func (job *RawJob) Bury() {
	if err := job.conn.Bury(job.id, beanstalkdPrioLow); err != nil {
		job.LogError("Could not bury job: " + err.Error())
	}
}

//getAge gets the age of the job (in seconds) from the stats
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

// logError function logs an error messagge regarding the job referenced by Job.
func (job *RawJob) LogError(a ...interface{}) {
	log.Print("Job ", job.id, ": Error: ", fmt.Sprint(a...))
}

// logInfo function logs an info messagge regarding the job referenced by Job.
func (job *RawJob) LogInfo(a ...interface{}) {
	log.Print("Job ", job.id, ": ", fmt.Sprint(a...))
}
