package beanstalkworker

import "log"
import "time"
import "github.com/kr/beanstalk"

// Worker represents a single process that is connecting to beanstalkd
// and is consuming jobs from one or more tubes.
type Worker struct {
	addr     string
	tubeSubs map[string]func(JobManager)
}

// NewWorker creates a new worker process,
// but does not actually connect to beanstalkd server yet.
func NewWorker(addr string, tubeSubs map[string]func(JobManager)) *Worker {
	return &Worker{
		addr:     addr,
		tubeSubs: tubeSubs,
	}
}

// Run activates the worker and attempts to maintain a connection to
// the beanstalkd server.
func (w *Worker) Run() {
	for {
		conn, err := beanstalk.Dial("tcp", w.addr)
		if err != nil {
			log.Print("Error connecting to beanstalkd: ", err)
			time.Sleep(5 * time.Second)
			continue
		}

		defer conn.Close()

		watchTubes := make([]string, 0, len(w.tubeSubs))
		for tube := range w.tubeSubs {
			watchTubes = append(watchTubes, tube)
		}
		tubes := beanstalk.NewTubeSet(conn, watchTubes...)
		log.Printf("Connected, watching %v for new jobs", watchTubes)

	loop:
		for {
			job := w.getNextJob(tubes)

			if job.err != nil {
				if job.err.Error() == "reserve-with-timeout: timeout" {
					continue
				} else if job.err.Error() == "reserve-with-timeout: deadline soon" {
					//Dont re-poll too often. This is important because otherwise we
					//end up in a busy wait loop for 1s spinning up go routines.
					time.Sleep(2 * time.Second)
					continue
				}

				//Some other problem so restart connection to beanstalkd.
				log.Print("Error getting job from tube: ", job.err)
				break loop
			}

			w.subHandler(job)
		}
		conn.Close() //We will reconnect in next loop iteration.
	}
}

// getNextJob retrieves the next job from the tubes being watched.
func (w *Worker) getNextJob(tubes *beanstalk.TubeSet) *RawJob {
	id, body, err := tubes.Reserve(60 * time.Second)
	job := &RawJob{
		id:   id,
		body: &body,
		err:  err,
		conn: tubes.Conn,
	}

	if err != nil {
		return job
	}

	//Look up tube info.
	stats, err := tubes.Conn.StatsJob(job.id)
	if err != nil {
		job.err = err
		return job
	}

	job.tube = stats["tube"]

	return job
}

func (w *Worker) subHandler(job *RawJob) {
	for tube, cb := range w.tubeSubs {
		if tube == job.GetTube() {
			cb(job)
		}
	}
}
