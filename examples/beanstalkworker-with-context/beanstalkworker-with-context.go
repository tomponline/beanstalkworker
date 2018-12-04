package main

import (
	"context"
	"flag"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"syscall"

	"github.com/tomponline/beanstalkworker"
)

const (
	appName = "beanstalkworker-with-context"
)

var (
	// Beanstalk related config
	beanstalkAddr = flag.String("beanstalkAddr", "127.0.0.1:11300", "Address of the beanstalk server")
	numWorkers    = flag.Int("numWorkers", 2, "Number of concurrent workers to run")
)

func main() {
	setUpSyslog(appName)

	// Log the start time and defer the finishing log line
	log.Printf("Starting %s", appName)
	defer log.Printf("Finished %s", appName)

	// Parse our config flags
	flag.Parse()

	// Setup a context for stopping the worker when the program exits
	ctx, cancel := context.WithCancel(context.Background())

	// Start the signal handler to catch system signals and shutdown cleanly
	go signalHandler(cancel)

	// Run the beanstalk worker
	runWorker(ctx)
}

//setUpSyslog sets up syslog with a application name suffix
func setUpSyslog(tag string) {
	log.SetFlags(0)
	syslogWriter, err := syslog.New(syslog.LOG_INFO, tag)
	if err == nil {
		log.SetOutput(syslogWriter)
	}
}

//signalHandler function responds to SIGUP, SIGTERM signals to cleanly shutdown worker using context
func signalHandler(cancel context.CancelFunc) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	for {
		<-sigc
		log.Print("Got signal, cancelling context")
		cancel()
	}
}

//runWorker runs the beanstalk worker with a context
func runWorker(ctx context.Context) {
	// Initialise the worker
	bsWorker := beanstalkworker.NewWorker(*beanstalkAddr)

	// Set the number of concurrent workers
	bsWorker.SetNumWorkers(*numWorkers)

	// Subscribe to jobs from our queue
	bsWorker.Subscribe("import-jobs", func(jobMgr beanstalkworker.JobManager, jobData ImportJobData) {
		// Set up a new 'import' job handler, attaching the job manager and job data to it
		// ImportJobHandler embeds the beanstalkworker.JobManager to expose job control methods
		jh := ImportJobHandler{
			JobManager: jobMgr,
			jobData:    jobData,
		}

		// Run the job handler for this job
		jh.Run()
	})

	// Run the worker, this call blocks until the context is cancelled
	// The beanstalkworker package takes care of reconnecting to beanstalk automatically
	bsWorker.Run(ctx)
}
