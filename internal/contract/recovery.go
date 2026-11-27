package contract

import (
	"context"
	"errors"
	"time"
)

// RecoveryConfig versions the host assembly, including all provider implementations.
type RecoveryConfig struct {
	Directory        string
	BindingVersion   string
	ReconcileTimeout time.Duration
}

var (
	ErrRecoveryUnavailable = errors.New("run recovery is unavailable")
	ErrRecoveryBusy        = errors.New("run is owned by another local runner")
	ErrRecoveryStorage     = errors.New("run checkpoint is unavailable")
	ErrRecoveryBinding     = errors.New("run recovery binding has changed")
	ErrRecoveryObservation = errors.New("run observation cannot be restored")
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

type ReceiptKey struct {
	Runner           RunnerRef
	Subject          SubjectID
	RunID            RunID
	Step             uint32
	CallID           CallID
	Tool             ToolRef
	BindingVersion   string
	Environment      EnvironmentRef
	Workspace        WorkspaceRef
	WorkspaceVersion string
	Fingerprint      string
}

type ToolReceipt struct {
	Key    ReceiptKey
	Result ToolResult
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
