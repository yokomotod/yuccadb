package logger

import "log"

type Logger interface {
	Tracef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
}

type LogLevel int

const (
	Trace LogLevel = iota
	Debug
	Info
	Warning
	Error
)

type DefaultLogger struct {
	Level LogLevel
}

func (l *DefaultLogger) Tracef(format string, v ...interface{}) {
	if l.Level <= Trace {
		log.Printf("[TRACE] "+format, v...)
	}
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.Level <= Debug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	if l.Level <= Info {
		log.Printf("[INFO] "+format, v...)
	}
}
