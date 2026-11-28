package contract

import (
	"context"

	legacy "github.com/shubolab/superagent/internal/contract"
)

type (
	ArtifactRef           = legacy.ArtifactRef
	ArtifactPage          = legacy.ArtifactPage
	ArtifactOwner         = legacy.ArtifactOwner
	ArtifactCall          = legacy.ArtifactCall
	ArtifactWriter        = legacy.ArtifactWriter
	ArtifactStore         = legacy.ArtifactStore
	RetainedArtifactStore = legacy.RetainedArtifactStore
	TakeoverArtifactStore = legacy.TakeoverArtifactStore
	DurableArtifactStore  = legacy.DurableArtifactStore
	ArtifactOperation     = legacy.ArtifactOperation
	ArtifactAccess        = legacy.ArtifactAccess
)

const ArtifactMaxBlockBytes = legacy.ArtifactMaxBlockBytes

const (
	ArtifactReserve            = legacy.ArtifactReserve
	ArtifactOpenWriter         = legacy.ArtifactOpenWriter
	ArtifactSeal               = legacy.ArtifactSeal
	ArtifactPreparePublication = legacy.ArtifactPreparePublication
	ArtifactPublish            = legacy.ArtifactPublish
	ArtifactDiscard            = legacy.ArtifactDiscard
	ArtifactCheck              = legacy.ArtifactCheck
	ArtifactRead               = legacy.ArtifactRead
)

var (
	ErrArtifactUnavailable = legacy.ErrArtifactUnavailable
	ErrArtifactCapacity    = legacy.ErrArtifactCapacity
	ErrArtifactOutputLimit = legacy.ErrArtifactOutputLimit
	ErrArtifactReceipt     = legacy.ErrArtifactReceipt
	ErrArtifactCleanup     = legacy.ErrArtifactCleanup
)

// VerifyArtifactAccess checks the authorization for this storage operation.
// For read and check, pass ArtifactCall{Owner: owner}. For reserve, open and
// discard, pass a zero ref. Recheck during the final storage transaction.
// ReceiptKey pins the original admission; Epoch may be newer during recovery.
func VerifyArtifactAccess(ctx context.Context, op ArtifactOperation, call ArtifactCall, ref ArtifactRef) (ArtifactAccess, error) {
	return legacy.VerifyArtifactAccess(ctx, op, call, ref)
}

// SealArtifact authorizes the writer opened for call to seal ref. The callback
// must synchronously commit only its metadata. Its context is scoped to that
// commit. Write and sync immutable bytes before calling SealArtifact.
func SealArtifact(ctx context.Context, call ArtifactCall, ref ArtifactRef, commit func(context.Context) error) error {
	return legacy.SealArtifact(ctx, call, ref, commit)
}
