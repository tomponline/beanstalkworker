package main

import (
	"beanstalkworker/beanstalkworker"
	"os"
	"os/signal"
	"syscall"
	"context"
)

var log = beanstalkworker.NewStdLogger()

func main() {


	//Setup context for cancelling beanstalk worker.
	ctx, cancel := context.WithCancel(context.Background())

	//Start up signal handler that will cleanly shutdown beanstalk worker.
	go signalHandler(cancel)

	//Define a new worker process - how to connect to the beanstalkd server.
	bsWorker := beanstalkworker.NewWorker("127.0.0.1:11300")

	//Set concurrent worker threads to 2.
	bsWorker.SetNumWorkers(2)

	//Define a common value (example a shared database connection)
	commonVar := "some common value"

	//Add one or more subcriptions to specific tubes with a handler function.
	bsWorker.Subscribe("job1", func(jobMgr beanstalkworker.JobManager, jobData Job1Data) {
		//Create a fresh handler struct per job (this ensures fresh state for each job).
		handler := &Job1Handler{
			JobManager: jobMgr,    //Embed the JobManager into the handler.
			commonVar:  commonVar, //Pass the commonVar into the handler.
		}

		handler.Run(jobData)
	})

	//Run the beanstalk worker, this blocks until the context is cancelled.
	//It will also handle reconnecting to beanstalkd server automatically.
	bsWorker.Run(ctx)
}

func signalHandler(cancel context.CancelFunc) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for {
		<-sigc
		log.Info("Got signal, cancelling context")
		cancel()
	}
}
