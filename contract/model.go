package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	ModelRef          = legacy.ModelRef
	Message           = legacy.Message
	UserMessage       = legacy.UserMessage
	AssistantMessage  = legacy.AssistantMessage
	ModelProvider     = legacy.ModelProvider
	ModelRequest      = legacy.ModelRequest
	ModelStream       = legacy.ModelStream
	ModelEventKind    = legacy.ModelEventKind
	ModelEvent        = legacy.ModelEvent
	ModelFinishReason = legacy.ModelFinishReason
	ModelReply        = legacy.ModelReply
)

const (
	ModelTextDelta       = legacy.ModelTextDelta
	ModelCompleted       = legacy.ModelCompleted
	ModelFinishStop      = legacy.ModelFinishStop
	ModelFinishToolCalls = legacy.ModelFinishToolCalls
)

var (
	ErrModelAuthFailed        = legacy.ErrModelAuthFailed
	ErrModelNotFound          = legacy.ErrModelNotFound
	ErrModelResponseTruncated = legacy.ErrModelResponseTruncated
	ErrModelResponseProtocol  = legacy.ErrModelResponseProtocol
	ErrModelOutputLimit       = legacy.ErrModelOutputLimit
)
