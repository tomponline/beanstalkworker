package main

import "github.com/tomponline/beanstalkworker"
import "context"
import "os"
import "os/signal"
import "syscall"
import "log"
import "time"

type Job2 struct {
	Cmd string `json:"cmd"`
}

func main() {
	//Setup context for cancelling beanstalk worker.
	ctx, cancel := context.WithCancel(context.Background())

	//Start up signal handler that will cleanly shutdown beanstalk worker.
	go signalHandler(cancel)

	//Define a new worker process, how to connect to the beanstalkd server,
	//and which tubes to watch.
	bsWorker := beanstalkworker.NewWorker("127.0.0.1:11300")
	bsWorker.Subscribe("Job1", NewJob1Handler)
	bsWorker.Subscribe("Job2", NewJob2Handler)
	bsWorker.Run(ctx)
}

// NewJob1Handler providdes a handler for the "Job1" command type.
func NewJob1Handler(job beanstalkworker.JobManager, jobData map[string]string) {
	job.LogInfo("Test job 1 callback: ", jobData, job.GetTube())
	time.Sleep(15 * time.Second)
	job.LogInfo("Finished!")
	job.Delete() //Finished process job, delete from queue.
}

// NewJob2Handler providdes a handler for the "Job2" command type.
func NewJob2Handler(job beanstalkworker.JobManager, jobData Job2) {
	job.LogInfo("Test job 2 callback: ", jobData, job.GetPriority(), job.GetAge())
	job.SetReturnPriority(1023)
	job.SetReturnDelay(time.Second * 60)
	job.Release() //Something is wrong, release job back for processing later.
}

func signalHandler(cancel context.CancelFunc) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for {
		<-sigc
		log.Print("Got signal, cancelling context")
		cancel()
	}
}
