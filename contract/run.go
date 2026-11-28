// Package contract defines the shared API for SuperAgent components and hosts.
// Aliases preserve the identity of types used by the original SDK API.
package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	RunID            = legacy.RunID
	RunnerRef        = legacy.RunnerRef
	SubjectID        = legacy.SubjectID
	Budget           = legacy.Budget
	RunnerConfig     = legacy.RunnerConfig
	StartRequest     = legacy.StartRequest
	EventStream      = legacy.EventStream
	EventKind        = legacy.EventKind
	Event            = legacy.Event
	AssistantPreview = legacy.AssistantPreview
	RunOutcome       = legacy.RunOutcome
	Usage            = legacy.Usage
	StopCode         = legacy.StopCode
	StopDetail       = legacy.StopDetail
	Result           = legacy.Result
)

const (
	EventRunStarted       = legacy.EventRunStarted
	EventTextDelta        = legacy.EventTextDelta
	EventMessageCompleted = legacy.EventMessageCompleted
	EventRunFinished      = legacy.EventRunFinished
	EventToolDispatched   = legacy.EventToolDispatched
	EventToolCompleted    = legacy.EventToolCompleted
)

const (
	Succeeded          = legacy.Succeeded
	Cancelled          = legacy.Cancelled
	TimedOut           = legacy.TimedOut
	BudgetExceeded     = legacy.BudgetExceeded
	Failed             = legacy.Failed
	ToolOutcomeUnknown = legacy.ToolOutcomeUnknown
)

const (
	StopCancelled                     = legacy.StopCancelled
	StopDeadline                      = legacy.StopDeadline
	StopModelAuthFailed               = legacy.StopModelAuthFailed
	StopModelNotFound                 = legacy.StopModelNotFound
	StopModelFailure                  = legacy.StopModelFailure
	StopModelTruncated                = legacy.StopModelTruncated
	StopModelProtocol                 = legacy.StopModelProtocol
	StopModelInputLimit               = legacy.StopModelInputLimit
	StopModelOutputLimit              = legacy.StopModelOutputLimit
	StopModelRequestLimit             = legacy.StopModelRequestLimit
	StopToolRequestLimit              = legacy.StopToolRequestLimit
	StopToolRawOutputLimit            = legacy.StopToolRawOutputLimit
	StopToolScanLimit                 = legacy.StopToolScanLimit
	StopToolReceiptLimit              = legacy.StopToolReceiptLimit
	StopToolUnknown                   = legacy.StopToolUnknown
	StopEnvironmentCapability         = legacy.StopEnvironmentCapability
	StopEnvironmentFailure            = legacy.StopEnvironmentFailure
	StopContextMeasurementUnavailable = legacy.StopContextMeasurementUnavailable
	StopContextInputChanged           = legacy.StopContextInputChanged
	StopModelContextOverflow          = legacy.StopModelContextOverflow
	StopContextWindowLimit            = legacy.StopContextWindowLimit
)

var (
	ErrInvalidConfiguration   = legacy.ErrInvalidConfiguration
	ErrBindingNotFound        = legacy.ErrBindingNotFound
	ErrAccessDenied           = legacy.ErrAccessDenied
	ErrInvalidRequest         = legacy.ErrInvalidRequest
	ErrStartOutcomeUnknown    = legacy.ErrStartOutcomeUnknown
	ErrStartConflict          = legacy.ErrStartConflict
	ErrRunNotFound            = legacy.ErrRunNotFound
	ErrObservationAlreadyUsed = legacy.ErrObservationAlreadyUsed
	ErrObserveOverflow        = legacy.ErrObserveOverflow
	ErrStreamClosed           = legacy.ErrStreamClosed
)
