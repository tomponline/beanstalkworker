package beanstalkworker

import "log"
import "time"
import "github.com/kr/beanstalk"

type Worker struct {
	addr       string
	watchTubes []string
}

// NewWorker creates a new worker process,
// but does not actually connect to beanstalkd server yet.
func NewWorker(addr string, watchTubes []string) *Worker {
	return &Worker{
		addr:       addr,
		watchTubes: watchTubes,
	}
}

// Run activates the worker and attempts to maintain a connection to
// the beanstalkd server.
func (w *Worker) Run(cb func(job *RawJob)) {
	for {
		conn, err := beanstalk.Dial("tcp", w.addr)

		if err != nil {
			log.Print("Error connecting to beanstalkd: ", err)
			time.Sleep(5 * time.Second)
			continue
		}

		defer conn.Close()

		tubes := beanstalk.NewTubeSet(conn, w.watchTubes...)
		log.Printf("Connected, watching %v for new jobs", w.watchTubes)

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

			cb(job) //Pass to callback handler.
		}
		conn.Close() //We will reconnect in next loop iteration.
	}
}

func (w *Worker) getNextJob(tubes *beanstalk.TubeSet) *RawJob {
	id, body, err := tubes.Reserve(60 * time.Second)
	job := &RawJob{
		id:   id,
		body: &body,
		err:  err,
		conn: tubes.Conn,
	}

	return job
}
