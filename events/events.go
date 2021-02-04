package events

import "os"

// CancelEvent is an event triggered by the user when he wants to terminate safely the execution of the connector.
type CancelEvent struct {
	Signal os.Signal
}

// ExitEvent represents the definitive exit of the task.
type ExitEvent struct {
	Code uint8
}

// ErrorEvent represents an error thrown by the goroutine of the task that is recoverable by retrying.
type ErrorEvent struct {
	Err error
}