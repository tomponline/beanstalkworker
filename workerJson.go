package beanstalkworker

import "log"
import "encoding/json"

type JSONCmdWorker struct {
	Worker
	subs map[string]func(JobDecoded)
}

// NewJsonWorker creates a new JSON Command worker process,
// but does not actually connect to beanstalkd server yet.
func NewJSONCmdWorker(worker *Worker) *JSONCmdWorker {
	return &JSONCmdWorker{
		Worker: *worker,
		subs:   make(map[string]func(JobDecoded)),
	}
}

// Run activates the worker and attempts to maintain a connection to
// the beanstalkd server.
func (w *JSONCmdWorker) Run() {
	log.Println("JSON Command worker starting")
	w.Worker.Run(w.HandleRawJob) //Register with basic worker job handler.
}

func (w *JSONCmdWorker) Subscribe(cmd string, cb func(JobDecoded)) {
	w.subs[cmd] = cb
}

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
