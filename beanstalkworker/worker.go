package beanstalkworker

import (
	"time"
	"github.com/tomponline/beanstalk"
	"encoding/json"
	"reflect"
	"context"
	"strconv"
	"sync"
)

// Handler provides an interface type for callback functions.
type Handler interface{}

// Worker represents a single process that is connecting to beanstalkd
// and is consuming jobs from one or more tubes.
type Worker struct {
	addr       string
	tubeSubs   map[string]func(*RawJob)
	numWorkers int
	wg         sync.WaitGroup
	log        Logger
}

// NewWorker creates a new worker process,
// but does not actually connect to beanstalkd server yet.
func NewWorker(addr string) *Worker {
	return &Worker{
		addr:     addr,
		tubeSubs: make(map[string]func(*RawJob)),
		log:      NewStdLogger(),
	}
}

// SetNumWorkers sets the number of concurrent workers threads that should be started.
// Each thread establishes a separate connection to the beanstalkd server.
func (w *Worker) SetNumWorkers(numWorkers int) {
	w.numWorkers = numWorkers
}

// Subscribe adds a handler function to be run for jobs coming from a particular tube.
func (w *Worker) Subscribe(tube string, cb Handler) {
	w.tubeSubs[tube] = func(job *RawJob) {
		jobVal := reflect.ValueOf(job)
		cbFunc := reflect.ValueOf(cb)
		cbType := reflect.TypeOf(cb)
		if cbType.Kind() != reflect.Func {
			panic("Handler needs to be a func")
		}

		dataType := cbType.In(1)
		dataPtr := reflect.New(dataType)

		if err := json.Unmarshal(*job.body, dataPtr.Interface()); err != nil {
			job.LogError("Error decoding JSON for job: ", err, ", '", string(*job.body), "', releasing...")
			job.Release()
			return
		}

		cbFunc.Call([]reflect.Value{jobVal, reflect.Indirect(dataPtr)})
	}
}

// Run starts one or more worker threads based on the numWorkers value.
// If numWorkers is set to zero or less then 1 worker is started.
func (w *Worker) Run(ctx context.Context) {
	if w.numWorkers <= 0 {
		w.numWorkers = 1
	}

	for i := 0; i < w.numWorkers; i++ {
		w.wg.Add(1) //Increment wait group count to represent new worker.
		go w.startWorker(ctx)
	}

	w.wg.Wait() //Block here until all workers cleanly finish.
}

// startWorker activates a single worker and attempts to maintain a connection to the beanstalkd server.
func (w *Worker) startWorker(ctx context.Context) {
	defer w.log.Info("Worker stopped!")
	defer w.wg.Done()

	for {
		//Check the process hasn't been cancelled whilst we are connecting.
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := beanstalk.Dial("tcp", w.addr)
		if err != nil {
			w.log.Info("Error connecting to beanstalkd: ", err)
			time.Sleep(5 * time.Second)
			continue
		}

		defer conn.Close()

		watchTubes := make([]string, 0, len(w.tubeSubs))
		for tube := range w.tubeSubs {
			watchTubes = append(watchTubes, tube)
		}
		tubes := beanstalk.NewTubeSet(conn, watchTubes...)
		w.log.Infof("Connected, watching %v for new jobs", watchTubes)
		jobCh := make(chan *RawJob)

	loop:
		for {
			go w.getNextJob(jobCh, tubes)
			select {
			case <-ctx.Done():
				//Context has been cancelled, time to finish up.
				return
			case job := <-jobCh:
				//Handle job from the beanstalkd server.
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
					w.log.Info("Error getting job from tube: ", job.err)
					break loop
				}

				w.subHandler(job)
			}

		}
		conn.Close() //We will reconnect in next loop iteration.
	}
}

// getNextJob retrieves the next job from the tubes being watched.
func (w *Worker) getNextJob(jobCh chan *RawJob, tubes *beanstalk.TubeSet) {
	id, body, err := tubes.Reserve(60 * time.Second)
	job := &RawJob{
		id:   id,
		body: &body,
		err:  err,
		conn: tubes.Conn,
		log:  w.log,
	}

	if err != nil {
		jobCh <- job
		return
	}

	//Look up job stats.
	stats, err := tubes.Conn.StatsJob(job.id)
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}

	//Cache tube job was received from in the job.
	job.tube = stats["tube"]

	///Convert string age into time.Duration and cache in job.
	age, err := strconv.Atoi(stats["age"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}

	job.age = time.Duration(age) * time.Second

	///Convert string delay into time.Duration and cache in job.
	delay, err := strconv.Atoi(stats["delay"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}

	job.delay = time.Duration(delay) * time.Second

	//Initialise the return delay as the current delay.
	job.returnDelay = job.delay

	//If the initial returnDelay is 0s, then set to 60s.
	//This ensures that if job umarshalling fails that we don't get the job
	//repeatedly re-released without any delay.
	//If you do need a 0s delay, use SetReturnDelay() in the handler function.
	if job.returnDelay <= 0 {
		job.returnDelay = 60 * time.Second
	}

	//Convert string priority into uint32 and cache in job.
	prio, err := strconv.Atoi(stats["pri"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}
	job.prio = uint32(prio)

	//Initialise the return priority as the current priority.
	job.returnPrio = job.prio

	//Convert string releases into uint32 and cache in job.
	releases, err := strconv.Atoi(stats["releases"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}
	job.releases = uint32(releases)

	//Convert string reserves into uint32 and cache in job.
	reserves, err := strconv.Atoi(stats["reserves"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}
	job.reserves = uint32(reserves)

	//Convert string timeouts into uint32 and cache in job.
	timeouts, err := strconv.Atoi(stats["timeouts"])
	if err != nil {
		job.err = err
		jobCh <- job
		return
	}
	job.timeouts = uint32(timeouts)

	//Send the job to the receiver channel.
	jobCh <- job
}

// subHandler finds and executes any subcriber function for a job.
func (w *Worker) subHandler(job *RawJob) {
	tube := job.GetTube()
	if cb, ok := w.tubeSubs[tube]; ok {
		cb(job)
	} else {
		panic("Should not get a job with no handler function")
	}
}
