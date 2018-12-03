package beanstalkworker_test

import "github.com/tomponline/beanstalkworker"
import "context"
import "os"
import "os/signal"
import "syscall"
import "log"
import "fmt"
import "time"

func Example_worker() {
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

//signalHandler catches OS signals for program to end.
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

//MyLogger provides custom logging.
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

//Job Handler

// Job1Handler contains the business logic to handle the Job1 type jobs.
type Job1Handler struct {
	beanstalkworker.JobManager
	commonVar string
}

// Job1Data is a struct that represents the Job1 data that arrives from the queue.
type Job1Data struct {
	SomeField      string `json:"someField"`
	SomeOtherField int    `json:"someOtherField"`
}

// LogError example of overriding a function provided in beanstalkworker.JobManager
// and calling the underlying function in order to add context.
func (handler *Job1Handler) LogError(a ...interface{}) {
	handler.JobManager.LogError("Job1 error: ", fmt.Sprint(a...))
}

// Run is executed by the beanstalk worker when a Job1 type job is received.
func (handler *Job1Handler) Run(jobData Job1Data) {
	handler.LogInfo("Starting job with commonVar value: ", handler.commonVar)
	handler.LogInfo("Job Data received: ", jobData)
	handler.LogInfo("Job Priority: ", handler.GetPriority())
	handler.LogInfo("Job Releases: ", handler.GetReleases())
	handler.LogInfo("Job Reserves: ", handler.GetReserves())
	handler.LogInfo("Job Age: ", handler.GetAge())
	handler.LogInfo("Job Delay: ", handler.GetDelay())
	handler.LogInfo("Job Timeouts: ", handler.GetTimeouts())
	handler.LogInfo("Job Tube: ", handler.GetTube())
	// Retrieve the server's hostname where the job is running
	conn := handler.GetConn()
	stats, err := conn.Stats()
	if err != nil {
		handler.Release()
		return
	}
	handler.LogInfo("Hostname: ", stats["hostname"])

	//Simulate job processing time
	time.Sleep(2 * time.Second)

	if handler.GetTimeouts() == 0 {
		handler.LogInfo("Simulating a timeout by not releasing/deleting job")
		return
	}

	if handler.GetReserves() == 2 {
		handler.LogInfo("Release without setting custom delay or priority")
		handler.Release()
		return
	}

	handler.SetReturnDelay(5 * time.Second) //Optional return delay (defaults to current delay)
	handler.SetReturnPriority(5)            //Optional return priority (defaults to current priority)

	if handler.GetReleases() >= 3 {
		handler.Delete()
		handler.LogError("Deleting job as too many releases")
		return
	}

	handler.LogInfo("Releasing job to be retried...")
	handler.Release() //Pretend job process failed and needs retrying
}
