package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	ToolRef               = legacy.ToolRef
	CallID                = legacy.CallID
	ToolPermission        = legacy.ToolPermission
	ToolSpec              = legacy.ToolSpec
	LocalToolBinding      = legacy.LocalToolBinding
	ModelTool             = legacy.ModelTool
	ModelToolCall         = legacy.ModelToolCall
	ToolMessage           = legacy.ToolMessage
	ToolLimits            = legacy.ToolLimits
	Tool                  = legacy.Tool
	ToolRequest           = legacy.ToolRequest
	ToolExecutionState    = legacy.ToolExecutionState
	ToolCompletionOutcome = legacy.ToolCompletionOutcome
	ToolResult            = legacy.ToolResult
	ToolCompletion        = legacy.ToolCompletion
	ToolReason            = legacy.ToolReason
	ToolUsage             = legacy.ToolUsage
	ToolStatus            = legacy.ToolStatus
	ToolEvent             = legacy.ToolEvent
	CleanupStatus         = legacy.CleanupStatus
)

const (
	ToolExecutionKnown       = legacy.ToolExecutionKnown
	ToolExecutionNotExecuted = legacy.ToolExecutionNotExecuted
	ToolExecutionUnknown     = legacy.ToolExecutionUnknown
	ToolSucceeded            = legacy.ToolSucceeded
	ToolFailed               = legacy.ToolFailed
	ToolCancelled            = legacy.ToolCancelled
	ToolStatusSucceeded      = legacy.ToolStatusSucceeded
	ToolStatusCancelled      = legacy.ToolStatusCancelled
	ToolStatusFailed         = legacy.ToolStatusFailed
	ToolStatusRejected       = legacy.ToolStatusRejected
	ToolStatusUnknown        = legacy.ToolStatusUnknown
	CleanupReleased          = legacy.CleanupReleased
	CleanupPending           = legacy.CleanupPending
)

// ErrNotDelivered indicates that the call was never handed off for tool execution.
// A lost response after a possible handoff leaves the outcome unknown.
var ErrNotDelivered = legacy.ErrNotDelivered
