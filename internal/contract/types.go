// Package contract contains the types shared by the SDK, Mesh, and runner.
package contract

import (
	"context"
	"errors"
	"time"
)

type RunID string
type RunnerRef string
type ModelRef string
type SubjectID string

type Budget struct {
	Timeout            *time.Duration
	MaxModelRequests   *uint64
	MaxModelInputBytes *uint64
	MaxOutputBytes     *uint64
}

type RunnerConfig struct {
	AllowedModels      map[SubjectID][]ModelRef
	DefaultBudget      Budget
	BudgetCaps         Budget
	ObserveBufferBytes uint64
	ResultRetention    time.Duration
}

type StartRequest struct {
	ID          RunID
	Model       ModelRef
	Instruction string
	History     []Message
	Input       UserMessage
	Budget      Budget
}

type Message struct {
	User      *UserMessage
	Assistant *AssistantMessage
}

type UserMessage struct {
	Text string
}

type AssistantMessage struct {
	Text string
}

type ModelProvider interface {
	Generate(ctx context.Context, req ModelRequest) (ModelStream, error)
}

type ModelRequest struct {
	RunID          RunID
	Step           uint32
	Model          ModelRef
	Instruction    string
	Messages       []Message
	MaxOutputBytes uint64
}

// ModelStream has one reader. Close is idempotent and unblocks Recv.
type ModelStream interface {
	Recv() (ModelEvent, error)
	Close() error
}

type ModelEventKind string

const (
	ModelTextDelta ModelEventKind = "text.delta"
	ModelCompleted ModelEventKind = "completed"
)

type ModelEvent struct {
	Kind      ModelEventKind
	TextDelta string
	Reply     *ModelReply
}

type ModelFinishReason string

const (
	ModelFinishStop      ModelFinishReason = "stop"
	ModelFinishToolCalls ModelFinishReason = "tool_calls"
)

type ModelReply struct {
	Finish ModelFinishReason
	Text   string
	Usage  *Usage
}

type Usage struct {
	InputTokens  uint64
	OutputTokens uint64
}

// EventStream has one reader. Close only ends the observation.
type EventStream interface {
	Recv() (Event, error)
	Close() error
}

type EventKind string

const (
	EventRunStarted       EventKind = "run.started"
	EventTextDelta        EventKind = "text.delta"
	EventMessageCompleted EventKind = "message.completed"
	EventRunFinished      EventKind = "run.finished"
)

type Event struct {
	RunID     RunID
	Seq       uint64
	Step      uint32
	Kind      EventKind
	TextDelta string
	Message   *AssistantPreview
	Outcome   RunOutcome
}

type AssistantPreview struct {
	Text string
}

type RunOutcome string

const (
	Succeeded      RunOutcome = "Succeeded"
	Cancelled      RunOutcome = "Cancelled"
	TimedOut       RunOutcome = "TimedOut"
	BudgetExceeded RunOutcome = "BudgetExceeded"
	Failed         RunOutcome = "Failed"
)

type StopCode string

const (
	StopCancelled        StopCode = "run_cancelled"
	StopDeadline         StopCode = "run_deadline"
	StopModelFailure     StopCode = "model_request_failed"
	StopModelTruncated   StopCode = "model_truncated"
	StopModelProtocol    StopCode = "model_protocol_error"
	StopModelInputLimit  StopCode = "model_input_limit"
	StopModelOutputLimit StopCode = "model_output_limit"
)

type StopDetail struct {
	Code       StopCode
	Step       uint32
	LimitBytes uint64
}

type Result struct {
	RunID    RunID
	Outcome  RunOutcome
	Answer   *AssistantMessage
	Messages []Message
	Usage    *Usage
	Stop     *StopDetail
}

var (
	ErrInvalidConfiguration   = errors.New("invalid configuration")
	ErrBindingNotFound        = errors.New("binding not found")
	ErrAccessDenied           = errors.New("access denied")
	ErrInvalidRequest         = errors.New("invalid request")
	ErrStartConflict          = errors.New("start request conflicts with an existing run")
	ErrRunNotFound            = errors.New("run not found")
	ErrObservationAlreadyUsed = errors.New("run observation already used")
	ErrObserveOverflow        = errors.New("observation buffer overflow")
	ErrStreamClosed           = errors.New("stream closed")
)
