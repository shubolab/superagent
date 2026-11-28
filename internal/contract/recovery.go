package contract

import (
	"context"
	"errors"
	"time"
)

// RecoveryConfig versions the host assembly, including all provider implementations.
type RecoveryConfig struct {
	Takeover         *TakeoverConfig
	Directory        string
	BindingVersion   string
	ReconcileTimeout time.Duration
}

// TakeoverConfig enables lease-based ownership for explicitly opted-in Runs.
type TakeoverConfig struct {
	LeaseDuration time.Duration
}

var (
	ErrRecoveryOwnershipLost = errors.New("run ownership lease is no longer valid")
	ErrRecoveryRevision      = errors.New("run checkpoint revision changed")
	ErrRecoveryUnavailable   = errors.New("run recovery is unavailable")
	ErrRecoveryBusy          = errors.New("run is owned by another local runner")
	ErrRecoveryStorage       = errors.New("run checkpoint is unavailable")
	ErrRecoveryBinding       = errors.New("run recovery binding has changed")
	ErrRecoveryObservation   = errors.New("run observation cannot be restored")
)

const (
	StopModelInterrupted     StopCode = "model_step_interrupted"
	StopReconcileDeadline    StopCode = "receipt_reconciliation_deadline"
	StopWorkspaceUnavailable StopCode = "recovery_workspace_unavailable"
)

// RecoveryWorkspace returns a stable workspace identity after checking that old writes are fenced.
type RecoveryWorkspace interface {
	RecoveryWorkspace(context.Context, RunScope) (string, error)
}

// RetainedArtifactStore serves references committed by ArtifactWriter.Complete
// across process restarts, including a restart before the original call settles.
type RetainedArtifactStore interface {
	ArtifactStore
	RetainsAcrossRestart() bool
}

// RemoteToolIdentity pins a logical provider, its journal and an optional retained workspace.
type RemoteToolIdentity struct {
	Provider         string
	Journal          string
	Version          string
	Workspace        WorkspaceRef
	WorkspaceVersion string
	EvidenceRule     string `json:",omitempty"`
}

type RemoteOwner struct {
	Instance string
	Epoch    uint64
}

type ReceiptKey struct {
	Remote           RemoteToolIdentity `json:",omitzero"`
	Runner           RunnerRef
	Subject          SubjectID
	RunID            RunID
	Step             uint32
	AdmissionEpoch   uint64 `json:",omitempty"`
	CallID           CallID
	Tool             ToolRef
	BindingVersion   string
	Environment      EnvironmentRef
	Workspace        WorkspaceRef
	WorkspaceVersion string
	Fingerprint      string
}

type ToolReceipt struct {
	Key      ReceiptKey
	Result   ToolResult
	Cleanup  CleanupStatus     `json:",omitempty"`
	Evidence *WriteEvidenceRef `json:",omitempty"`
}

// ReceiptProvider queries the durable original invocation without starting work.
// Return nil for absent or pending records and honor the query context.
type ReceiptProvider interface {
	QueryReceipt(context.Context, ReceiptKey) (*ToolReceipt, error)
}

type receiptKeyContext struct{}

func WithReceiptKey(ctx context.Context, key ReceiptKey) context.Context {
	return context.WithValue(ctx, receiptKeyContext{}, key)
}

func InvocationReceiptKey(ctx context.Context) (ReceiptKey, bool) {
	key, ok := ctx.Value(receiptKeyContext{}).(ReceiptKey)
	return key, ok
}

// TakeoverWorkspace provides a managed publication point for workspaces
// whose tools only receive scoped access through Mesh.
type TakeoverWorkspace interface {
	RecoveryWorkspace
	FencesWorkspace(WorkspaceRef) bool
	PublishWorkspace(context.Context, CallScope, EnvironmentGrantRef, []byte) error
}

// TakeoverArtifactStore stages writes privately until Complete or Settle.
// Mesh fences those publication operations against ownership changes.
type TakeoverArtifactStore interface {
	RetainedArtifactStore
	StagesUntilPublication() bool
}

const MaxWorkspacePublishBytes = 64 << 10

type publisherKey struct{}

func WithWorkspacePublisher(ctx context.Context, publish func(context.Context, ToolRequest, string, []byte) error) context.Context {
	return context.WithValue(ctx, publisherKey{}, publish)
}

func PublishToolFile(ctx context.Context, req ToolRequest, path string, data []byte) error {
	publish, _ := ctx.Value(publisherKey{}).(func(context.Context, ToolRequest, string, []byte) error)
	if publish == nil {
		return ErrAccessDenied
	}
	return publish(ctx, req, path, data)
}

type workspacePublicationKey struct{}

type workspacePublication struct {
	call  CallScope
	grant EnvironmentGrantRef
}

func WithWorkspacePublication(ctx context.Context, call CallScope, grant EnvironmentGrantRef) context.Context {
	return context.WithValue(ctx, workspacePublicationKey{}, workspacePublication{call: call, grant: grant})
}

func HasWorkspacePublication(ctx context.Context, call CallScope, grant EnvironmentGrantRef) bool {
	publication, ok := ctx.Value(workspacePublicationKey{}).(workspacePublication)
	return ok && publication.call == call && publication.grant == grant
}
