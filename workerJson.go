package beanstalkworker

import "log"
import "encoding/json"

// JSONCmdWorker represents a worker process that handles JSON encoded jobs.
type JSONCmdWorker struct {
	Worker
	subs map[string]func(JobAccessor)
}

// NewJSONCmdWorker creates a new JSON Command worker process,
// but does not actually connect to beanstalkd server yet.
func NewJSONCmdWorker(worker *Worker) *JSONCmdWorker {
	return &JSONCmdWorker{
		Worker: *worker,
		subs:   make(map[string]func(JobAccessor)),
	}
}

// Run activates the worker and attempts to maintain a connection to
// the beanstalkd server.
func (w *JSONCmdWorker) Run() {
	log.Println("JSON Command worker starting")
	w.Worker.Run(w.HandleRawJob) //Register with basic worker job handler.
}

// Subscribe defines a handler function to process jobs where the "cmd" field
// in the job matches a specific command string subcribed to.
func (w *JSONCmdWorker) Subscribe(cmd string, cb func(JobAccessor)) {
	w.subs[cmd] = cb
}

// HandleRawJob defines a handler that is passed to the raw data worker in
// in order for jobs to be passed to the JSON worker.
func (w *JSONCmdWorker) HandleRawJob(rawJob *RawJob) {
	jsonJob := JSONJob{
		RawJob: *rawJob,
		fields: make(map[string]string),
	}

	if err := json.Unmarshal(*rawJob.body, &jsonJob.fields); err != nil {
		log.Printf("Error decoding JSON for job %v: %v", rawJob.id, err)
		rawJob.Release()
		log.Printf("Released job %v back to queue", rawJob.id)
		return
	}

	for cmd, cb := range w.subs {
		if cmd == jsonJob.GetField("cmd") {
			jsonJob.typeName = cmd
			cb(&jsonJob)
			return
		}
	}

	jsonJob.typeName = jsonJob.GetField("cmd")
	jsonJob.LogError("Unrecognised command, deleting...")
	jsonJob.Delete()
}
