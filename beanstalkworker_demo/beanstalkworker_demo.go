package main

import "github.com/tomponline/beanstalkworker/beanstalkworker"
import "context"
import "os"
import "os/signal"
import "syscall"
import "log"
import "fmt"

func main() {
	//Setup context for cancelling beanstalk worker.
	ctx, cancel := context.WithCancel(context.Background())

	//Start up signal handler that will cleanly shutdown beanstalk worker.
	go signalHandler(cancel)

	//Define a new worker process - how to connect to the beanstalkd server.
	bsWorker := beanstalkworker.NewWorker("127.0.0.1:11300")

	//Optional custom logger - see below.
	bsWorker.SetLogger(&MyLogger{})

	//Set concurrent worker threads to 2.
	bsWorker.SetNumWorkers(2)

	//Job is deleted from the queue if unmarshal error appears. We can
	//decide to bury or release (default behaviour) it as well.
	bsWorker.SetUnmarshalErrorAction(beanstalkworker.ActionDeleteJob)

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
		log.Print("Got signal, cancelling context")
		cancel()
	}
}

//Custom Logging Example

//MyLogger provides custom logging
type MyLogger struct {
}

//Info logs a custom info message regarding the job.
func (l *MyLogger) Info(v ...interface{}) {
	log.Print("MyInfo: ", fmt.Sprint(v...))
}

//Infof logs a custom info message regarding the job.
func (l *MyLogger) Infof(format string, v ...interface{}) {
	format = "MyInfof: " + format
	log.Print(fmt.Sprintf(format, v...))
}

//Error logs a custom error message regarding the job.
func (l *MyLogger) Error(v ...interface{}) {
	log.Print("MyError: ", fmt.Sprint(v...))
}

//Errorf logs a custom error message regarding the job.
func (l *MyLogger) Errorf(format string, v ...interface{}) {
	format = "MyErrorf: " + format
	log.Print(fmt.Sprintf(format, v...))
}
