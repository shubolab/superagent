package contract

import (
	"context"

	legacy "github.com/shubolab/superagent/internal/contract"
)

type (
	EvidenceWorkspace = legacy.EvidenceWorkspace
	WriteIntent       = legacy.WriteIntent
	StagedWrite       = legacy.StagedWrite
	FileVersion       = legacy.FileVersion
	WriteEvidence     = legacy.WriteEvidence
)

var ErrWriteEvidenceUnavailable = legacy.ErrWriteEvidenceUnavailable

// WriteEvidenceID identifies the record to look up. Verify the full intent when loading it.
func WriteEvidenceID(key ReceiptKey) string { return legacy.WriteEvidenceID(key) }

// VerifyWorkspaceWrite checks a provider-issued grant and the exact intent.
// Pass nil for staging and the prepared version for commit. Independently check
// the live workspace grant and enforce the storage transaction and byte limits.
func VerifyWorkspaceWrite(ctx context.Context, scope CallScope, grant EnvironmentGrantRef, intent WriteIntent, staged *StagedWrite) error {
	return legacy.VerifyWorkspaceWrite(ctx, scope, grant, intent, staged)
}

// VerifyWriteEvidenceRead checks permission to read the original intent's proof.
func VerifyWriteEvidenceRead(ctx context.Context, intent WriteIntent) error {
	return legacy.VerifyWriteEvidenceRead(ctx, intent)
}
