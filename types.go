// Package superagent runs agents through an embeddable Go SDK.
package superagent

import "github.com/shubolab/superagent/internal/contract"

type (
	RunID             = contract.RunID
	RunnerRef         = contract.RunnerRef
	ModelRef          = contract.ModelRef
	SubjectID         = contract.SubjectID
	Budget            = contract.Budget
	RunnerConfig      = contract.RunnerConfig
	StartRequest      = contract.StartRequest
	Message           = contract.Message
	UserMessage       = contract.UserMessage
	AssistantMessage  = contract.AssistantMessage
	ModelProvider     = contract.ModelProvider
	ModelRequest      = contract.ModelRequest
	ModelStream       = contract.ModelStream
	ModelEventKind    = contract.ModelEventKind
	ModelEvent        = contract.ModelEvent
	ModelFinishReason = contract.ModelFinishReason
	ModelReply        = contract.ModelReply
	EventStream       = contract.EventStream
	EventKind         = contract.EventKind
	Event             = contract.Event
	AssistantPreview  = contract.AssistantPreview
	RunOutcome        = contract.RunOutcome
	Usage             = contract.Usage
	StopCode          = contract.StopCode
	StopDetail        = contract.StopDetail
	Result            = contract.Result
)

const (
	ModelTextDelta        = contract.ModelTextDelta
	ModelCompleted        = contract.ModelCompleted
	ModelFinishStop       = contract.ModelFinishStop
	ModelFinishToolCalls  = contract.ModelFinishToolCalls
	EventRunStarted       = contract.EventRunStarted
	EventTextDelta        = contract.EventTextDelta
	EventMessageCompleted = contract.EventMessageCompleted
	EventRunFinished      = contract.EventRunFinished
	Succeeded             = contract.Succeeded
	Cancelled             = contract.Cancelled
	TimedOut              = contract.TimedOut
	BudgetExceeded        = contract.BudgetExceeded
	Failed                = contract.Failed
	StopCancelled         = contract.StopCancelled
	StopDeadline          = contract.StopDeadline
	StopModelFailure      = contract.StopModelFailure
	StopModelTruncated    = contract.StopModelTruncated
	StopModelProtocol     = contract.StopModelProtocol
	StopModelInputLimit   = contract.StopModelInputLimit
	StopModelOutputLimit  = contract.StopModelOutputLimit
)

var (
	ErrInvalidConfiguration   = contract.ErrInvalidConfiguration
	ErrBindingNotFound        = contract.ErrBindingNotFound
	ErrAccessDenied           = contract.ErrAccessDenied
	ErrInvalidRequest         = contract.ErrInvalidRequest
	ErrStartConflict          = contract.ErrStartConflict
	ErrRunNotFound            = contract.ErrRunNotFound
	ErrObservationAlreadyUsed = contract.ErrObservationAlreadyUsed
	ErrObserveOverflow        = contract.ErrObserveOverflow
	ErrStreamClosed           = contract.ErrStreamClosed
)
