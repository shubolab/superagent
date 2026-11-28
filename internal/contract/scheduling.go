package contract

import (
	"errors"
	"time"
)

// SessionSchedulingConfig limits concurrent Runs and waiting starts on one local runner.
type SessionSchedulingConfig struct {
	MaxActiveRuns int
	MaxQueuedRuns int
	MaxQueueWait  time.Duration
}

var (
	ErrSessionBusy               = errors.New("session has an uncommitted run")
	ErrScheduleQueueFull         = errors.New("session Run queue is full")
	ErrScheduleQueueTimeout      = errors.New("session Run queue wait expired")
	ErrScheduleAdmissionRequired = errors.New("runner requires scheduled session admission")
)

type SessionRunCheckpoint struct {
	Exists bool
	Result *Result
}
