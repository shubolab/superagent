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
	MaxToolRequests    *uint64
	MaxToolScanBytes   *uint64
}

type RunnerConfig struct {
	AllowedModels      map[SubjectID][]ModelRef
	ContextPolicies    map[ModelRef]ModelContextPolicy
	AllowedTools       map[SubjectID]map[ToolRef]ToolPermission
	DefaultBudget      Budget
	Tools              []ToolRef
	Environment        *EnvironmentRequirement
	BudgetCaps         Budget
	DefaultEnvironment EnvironmentRef
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
	Tools       []ToolRef
	Environment *EnvironmentRequirement
}

type Message struct {
	User      *UserMessage
	Assistant *AssistantMessage
	Tool      *ToolMessage `json:",omitempty"`
}

type UserMessage struct {
	Text string
}

type AssistantMessage struct {
	Text      string
	ToolCalls []ModelToolCall `json:",omitempty"`
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
	Tools          []ModelTool
	MaxOutputBytes uint64
	Context        *ModelContextOptions `json:",omitempty"`
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
	Finish    ModelFinishReason
	Text      string
	Usage     *Usage
	ToolCalls []ModelToolCall
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
	EventToolDispatched   EventKind = "tool.dispatched"
	EventToolCompleted    EventKind = "tool.completed"
)

type Event struct {
	RunID     RunID
	Seq       uint64
	Step      uint32
	Kind      EventKind
	TextDelta string
	Message   *AssistantPreview
	Outcome   RunOutcome
	Tool      *ToolEvent `json:",omitempty"`
}

type AssistantPreview struct {
	Text  string
	Calls []ToolEvent `json:",omitempty"`
}

type RunOutcome string

const (
	Succeeded          RunOutcome = "Succeeded"
	Cancelled          RunOutcome = "Cancelled"
	TimedOut           RunOutcome = "TimedOut"
	BudgetExceeded     RunOutcome = "BudgetExceeded"
	Failed             RunOutcome = "Failed"
	ToolOutcomeUnknown RunOutcome = "ToolOutcomeUnknown"
)

type StopCode string

const (
	StopCancelled          StopCode = "run_cancelled"
	StopDeadline           StopCode = "run_deadline"
	StopModelAuthFailed    StopCode = "model_auth_failed"
	StopModelNotFound      StopCode = "model_not_found"
	StopModelFailure       StopCode = "model_request_failed"
	StopModelTruncated     StopCode = "model_truncated"
	StopModelProtocol      StopCode = "model_protocol_error"
	StopModelInputLimit    StopCode = "model_input_limit"
	StopModelOutputLimit   StopCode = "model_output_limit"
	StopModelRequestLimit  StopCode = "model_request_limit"
	StopToolRequestLimit   StopCode = "tool_request_limit"
	StopToolScanLimit      StopCode = "tool_scan_limit"
	StopToolReceiptLimit   StopCode = "tool_receipt_limit"
	StopToolUnknown        StopCode = "tool_outcome_unknown"
	StopEnvironmentFailure StopCode = "environment_failed"
)

const (
	StopContextMeasurementUnavailable StopCode = "context_measurement_unavailable"
	StopContextInputChanged           StopCode = "context_input_changed"
	StopModelContextOverflow          StopCode = "model_context_overflow"
	StopContextWindowLimit            StopCode = "context_window_limit"
)

type StopDetail struct {
	Code        StopCode
	Step        uint32
	LimitBytes  uint64
	LimitTokens uint64
	CallID      CallID
}

type Result struct {
	RunID    RunID
	Outcome  RunOutcome
	Answer   *AssistantMessage
	Messages []Message
	Usage    *Usage
	Stop     *StopDetail
	Cleanup  CleanupStatus
}

var (
	ErrInvalidConfiguration   = errors.New("invalid configuration")
	ErrModelAuthFailed        = errors.New("model_auth_failed")
	ErrModelNotFound          = errors.New("model_not_found")
	ErrModelResponseTruncated = errors.New("model_truncated")
	ErrModelResponseProtocol  = errors.New("model_protocol_error")
	ErrModelOutputLimit       = errors.New("model_output_limit")
	ErrBindingNotFound        = errors.New("binding not found")
	ErrAccessDenied           = errors.New("access denied")
	ErrInvalidRequest         = errors.New("invalid request")
	ErrStartOutcomeUnknown    = errors.New("run start outcome unknown")
	ErrStartConflict          = errors.New("start request conflicts with an existing run")
	ErrRunNotFound            = errors.New("run not found")
	ErrObservationAlreadyUsed = errors.New("run observation already used")
	ErrObserveOverflow        = errors.New("observation buffer overflow")
	ErrStreamClosed           = errors.New("stream closed")
)
