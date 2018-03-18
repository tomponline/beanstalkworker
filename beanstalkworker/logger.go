package beanstalkworker

import "log"

type CustomLogger interface {
	Info(v ...interface{})
	Infof(format string, args ...interface{})
	Error(v ...interface{})
	Errorf(format string, args ...interface{})
}

type Logger struct {
	Info   func(v ...interface{})
	Infof  func(format string, v ...interface{})
	Error  func(v ...interface{})
	Errorf func(format string, v ...interface{})
}

// NewDefaultLogger creates a new Logger initialised to use the global log package.
func NewDefaultLogger() *Logger {
	return &Logger{
		Info:   log.Print,
		Infof:  log.Printf,
		Error:  log.Print,
		Errorf: log.Printf,
	}
}
