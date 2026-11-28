package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	RecoveryConfig          = legacy.RecoveryConfig
	TakeoverConfig          = legacy.TakeoverConfig
	RecoveryWorkspace       = legacy.RecoveryWorkspace
	TakeoverWorkspace       = legacy.TakeoverWorkspace
	RemoteToolIdentity      = legacy.RemoteToolIdentity
	RemoteOwner             = legacy.RemoteOwner
	ReceiptKey              = legacy.ReceiptKey
	ToolReceipt             = legacy.ToolReceipt
	ReceiptProvider         = legacy.ReceiptProvider
	WriteEvidenceRef        = legacy.WriteEvidenceRef
	SessionRunCheckpoint    = legacy.SessionRunCheckpoint
	SessionSchedulingConfig = legacy.SessionSchedulingConfig
)

const (
	StopModelInterrupted     = legacy.StopModelInterrupted
	StopReconcileDeadline    = legacy.StopReconcileDeadline
	StopWorkspaceUnavailable = legacy.StopWorkspaceUnavailable
	StopArtifactUnavailable  = legacy.StopArtifactUnavailable
	AppendFileEvidenceV1     = legacy.AppendFileEvidenceV1
)

var (
	ErrRecoveryOwnershipLost     = legacy.ErrRecoveryOwnershipLost
	ErrRecoveryUnavailable       = legacy.ErrRecoveryUnavailable
	ErrRecoveryBusy              = legacy.ErrRecoveryBusy
	ErrRecoveryStorage           = legacy.ErrRecoveryStorage
	ErrRecoveryBinding           = legacy.ErrRecoveryBinding
	ErrRecoveryObservation       = legacy.ErrRecoveryObservation
	ErrSessionBusy               = legacy.ErrSessionBusy
	ErrScheduleQueueFull         = legacy.ErrScheduleQueueFull
	ErrScheduleQueueTimeout      = legacy.ErrScheduleQueueTimeout
	ErrScheduleAdmissionRequired = legacy.ErrScheduleAdmissionRequired
)
