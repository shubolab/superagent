package contract

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type ToolRef string
type CallID string
type EnvironmentRef string
type WorkspaceRef string
type EnvironmentHandle string
type EnvironmentGrantRef string

type EnvironmentRequirement struct {
	Ref       EnvironmentRef
	Workspace WorkspaceRef
}

type ToolPermission struct {
	Environment EnvironmentRequirement
	ReadPrefix  string
}

type ToolSpec struct {
	Description     string
	InputSchema     json.RawMessage
	NeedsReadAccess bool
	Limits          ToolLimits
}

type LocalToolBinding struct {
	Spec ToolSpec
	Tool Tool
	// Prepare validates arguments before dispatch and returns the requested workspace path, if any.
	Prepare func(json.RawMessage) (string, error)
}

type ModelTool struct {
	Ref         ToolRef
	Description string
	InputSchema json.RawMessage
}

type ModelToolCall struct {
	CallID    CallID
	Tool      ToolRef
	Arguments json.RawMessage
}

type ToolMessage struct {
	Tool   ToolRef
	Result ToolResult
}

type ToolLimits struct {
	MaxScanBytes   uint64
	MaxResultBytes uint64
}

type Tool interface {
	Invoke(context.Context, ToolRequest) (ToolResult, error)
}

type ToolRequest struct {
	RunID            RunID
	CallID           CallID
	Tool             ToolRef
	Arguments        json.RawMessage
	EnvironmentGrant EnvironmentGrantRef
	Limits           ToolLimits
}

type ToolExecutionState string

const (
	ToolExecutionKnown       ToolExecutionState = "known"
	ToolExecutionNotExecuted ToolExecutionState = "not_executed"
	ToolExecutionUnknown     ToolExecutionState = "unknown"
)

type ToolCompletionOutcome string

const (
	ToolSucceeded ToolCompletionOutcome = "succeeded"
	ToolFailed    ToolCompletionOutcome = "failed"
	ToolCancelled ToolCompletionOutcome = "cancelled"
)

type ToolResult struct {
	RunID      RunID
	CallID     CallID
	State      ToolExecutionState
	Completion *ToolCompletion `json:",omitempty"`
	Reason     *ToolReason     `json:",omitempty"`
	Usage      *ToolUsage      `json:",omitempty"`
}

type ToolCompletion struct {
	Outcome ToolCompletionOutcome
	Content string `json:",omitempty"`
}

type ToolReason struct {
	Code string
}

type ToolUsage struct {
	ScannedBytes *uint64 `json:",omitempty"`
}

type ToolStatus string

const (
	ToolStatusSucceeded ToolStatus = "succeeded"
	ToolStatusFailed    ToolStatus = "failed"
	ToolStatusCancelled ToolStatus = "cancelled"
	ToolStatusRejected  ToolStatus = "rejected"
	ToolStatusUnknown   ToolStatus = "unknown"
)

type ToolEvent struct {
	CallID CallID
	Tool   ToolRef
	Status ToolStatus `json:",omitempty"`
}

type CleanupStatus string

const (
	CleanupReleased CleanupStatus = "released"
	CleanupPending  CleanupStatus = "pending"
)

type RunScope struct {
	Runner    RunnerRef
	Subject   SubjectID
	RunID     RunID
	Workspace WorkspaceRef
	Deadline  time.Time
}

type CallScope struct {
	Run    RunScope
	CallID CallID
	Tool   ToolRef
	Handle EnvironmentHandle
	Path   string
	Limits ToolLimits
}

type EnvironmentProvider interface {
	Allocate(context.Context, RunScope) (EnvironmentHandle, error)
	Grant(context.Context, CallScope, string) (EnvironmentGrantRef, error)
	OpenRead(context.Context, CallScope, EnvironmentGrantRef, string) (io.ReadCloser, error)
	Revoke(context.Context, CallScope, EnvironmentGrantRef) (uint64, error)
	Release(context.Context, RunScope, EnvironmentHandle) error
}

var (
	ErrReadLimit    = errors.New("file read limit reached")
	ErrNotDelivered = errors.New("tool request not delivered")
)

type fileOpenerKey struct{}

func WithFileOpener(ctx context.Context, open func(context.Context, ToolRequest, string) (io.ReadCloser, error)) context.Context {
	return context.WithValue(ctx, fileOpenerKey{}, open)
}

// OpenToolFile opens the one path authorized for this tool invocation.
func OpenToolFile(ctx context.Context, req ToolRequest, path string) (io.ReadCloser, error) {
	open, _ := ctx.Value(fileOpenerKey{}).(func(context.Context, ToolRequest, string) (io.ReadCloser, error))
	if open == nil {
		return nil, ErrAccessDenied
	}
	return open(ctx, req, path)
}

func JSONObject(raw json.RawMessage) bool {
	for _, b := range raw {
		switch b {
		case ' ', '\n', '\r', '\t':
			continue
		default:
			return b == '{' && json.Valid(raw)
		}
	}
	return false
}
