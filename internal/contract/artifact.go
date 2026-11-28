package contract

import (
	"context"
	"errors"
	"io"
	"time"
)

const ArtifactMaxBlockBytes = 64 << 10

type ArtifactRef struct {
	ID        string
	Version   string
	SizeBytes uint64
	SHA256    string
	ExpiresAt time.Time
}

type ArtifactPage struct {
	Content    []byte
	NextOffset uint64
	EOF        bool
}

type ArtifactOwner struct {
	Runner  RunnerRef
	Subject SubjectID
	RunID   RunID
}

type ArtifactCall struct {
	Owner    ArtifactOwner
	CallID   CallID
	Tool     ToolRef
	MaxBytes uint64
}

type ArtifactWriter interface {
	io.WriteCloser
	Complete() (ArtifactRef, error)
}

// ArtifactStore is the managed per-call output and retained artifact backend.
type ArtifactStore interface {
	Begin(context.Context, ArtifactCall) (uint64, error)
	OpenWriter(context.Context, ArtifactCall) (ArtifactWriter, error)
	Settle(context.Context, ArtifactCall, []ArtifactRef) (uint64, error)
	ReadArtifact(context.Context, ArtifactOwner, ArtifactRef, uint64, uint64) (ArtifactPage, error)
	Close() error
}

// DurableArtifactStore seals immutable bytes and publishes an authorized reference.
// A recoverable Run that produces artifacts requires this interface.
// The store ID must survive a restart of the same binding. PrepareArtifactPublication
// validates the original sealed bytes. Its returned function verifies the publish
// authorization, atomically marks the same reference visible and returns its byte
// count. A repeated confirmation returns the same count without publishing again.
type DurableArtifactStore interface {
	TakeoverArtifactStore
	ArtifactStoreID() string
	CheckArtifact(context.Context, ArtifactOwner, ArtifactRef) error
	PrepareArtifactPublication(context.Context, ArtifactCall, ArtifactRef) (func(context.Context) (uint64, error), error)
}

// ArtifactAdmission is supplied by the trusted Mesh, outside the public tool request.
type ArtifactAdmission struct {
	Operation ArtifactOperation
	Call      ArtifactCall
	Ref       ArtifactRef
	Deadline  time.Time
	Epoch     uint64
	Key       ReceiptKey
	Seal      func(context.Context, func() error) error
	Active    func() bool
	permit    *storagePermit
	sealUsed  *storagePermit
}

type ArtifactOperation uint8

const (
	ArtifactReserve ArtifactOperation = iota + 1
	ArtifactOpenWriter
	ArtifactSeal
	ArtifactPreparePublication
	ArtifactPublish
	ArtifactDiscard
	ArtifactCheck
	ArtifactRead
)

type ArtifactAccess struct {
	Deadline   time.Time
	Epoch      uint64
	ReceiptKey ReceiptKey
}

type artifactAdmissionKey struct{}

func WithArtifactAdmission(ctx context.Context, admission ArtifactAdmission) context.Context {
	return context.WithValue(ctx, artifactAdmissionKey{}, admission)
}

func ArtifactAdmissionFrom(ctx context.Context) (ArtifactAdmission, bool) {
	value, ok := ctx.Value(artifactAdmissionKey{}).(ArtifactAdmission)
	return value, ok
}

func WithScopedArtifactAdmission(ctx context.Context, admission ArtifactAdmission) (context.Context, func()) {
	admission.permit = &storagePermit{ctx: ctx}
	return WithArtifactAdmission(WithoutStorageRun(ctx), admission), admission.permit.revoke
}

func WithArtifactWriterAdmission(ctx context.Context, admission ArtifactAdmission) context.Context {
	admission.sealUsed = &storagePermit{ctx: ctx}
	return WithArtifactAdmission(WithoutStorageRun(ctx), admission)
}

func VerifyArtifactAccess(ctx context.Context, op ArtifactOperation, call ArtifactCall, ref ArtifactRef) (ArtifactAccess, error) {
	admission, ok := ArtifactAdmissionFrom(ctx)
	if !ok || op == 0 || admission.Operation != op || admission.Call != call || !sameArtifactAccessRef(admission.Ref, ref) ||
		(admission.permit != nil && !admission.permit.active()) || (admission.Active != nil && !admission.Active()) ||
		(op != ArtifactDiscard && !admission.Deadline.IsZero() && !time.Now().Before(admission.Deadline)) {
		return ArtifactAccess{}, ErrAccessDenied
	}
	if err := ctx.Err(); err != nil {
		return ArtifactAccess{}, err
	}
	return ArtifactAccess{Deadline: admission.Deadline, Epoch: admission.Epoch, ReceiptKey: admission.Key}, nil
}

func sameArtifactAccessRef(a, b ArtifactRef) bool {
	return a.ID == b.ID && a.Version == b.Version && a.SizeBytes == b.SizeBytes && a.SHA256 == b.SHA256 && a.ExpiresAt.Equal(b.ExpiresAt)
}

func SealArtifact(ctx context.Context, call ArtifactCall, ref ArtifactRef, commit func(context.Context) error) error {
	if _, err := VerifyArtifactAccess(ctx, ArtifactOpenWriter, call, ArtifactRef{}); err != nil {
		return err
	}
	admission, _ := ArtifactAdmissionFrom(ctx)
	if commit == nil || ref.ID == "" || ref.Version == "" || !ValidContentHash(ref.SHA256) || ref.SizeBytes > call.MaxBytes ||
		!time.Now().Before(ref.ExpiresAt) || (admission.Key != (ReceiptKey{}) && ref.ExpiresAt.Before(admission.Deadline)) {
		return ErrArtifactReceipt
	}
	if admission.sealUsed == nil || !admission.sealUsed.revoked.CompareAndSwap(false, true) {
		return ErrAccessDenied
	}
	fence := admission.Seal
	admission.Operation, admission.Ref, admission.Seal = ArtifactSeal, ref, nil
	seal := func() error {
		commitCtx, end := WithScopedArtifactAdmission(ctx, admission)
		defer end()
		if _, err := VerifyArtifactAccess(commitCtx, ArtifactSeal, call, ref); err != nil {
			return err
		}
		return commit(commitCtx)
	}
	if fence != nil {
		return fence(ctx, seal)
	}
	if admission.Epoch != 0 {
		return ErrAccessDenied
	}
	return seal()
}

var (
	ErrArtifactUnavailable = errors.New("artifact_unavailable")
	ErrArtifactCapacity    = errors.New("artifact_capacity")
	ErrArtifactOutputLimit = errors.New("artifact_output_limit")
	ErrArtifactCleanup     = errors.New("artifact_cleanup_pending")
	ErrArtifactReceipt     = errors.New("artifact_receipt_invalid")
)

type artifactOpenerKey struct{}

func WithArtifactOpener(ctx context.Context, open func(context.Context, ToolRequest) (ArtifactWriter, error)) context.Context {
	return context.WithValue(ctx, artifactOpenerKey{}, open)
}

func OpenToolArtifact(ctx context.Context, req ToolRequest) (ArtifactWriter, error) {
	open, _ := ctx.Value(artifactOpenerKey{}).(func(context.Context, ToolRequest) (ArtifactWriter, error))
	if open == nil {
		return nil, ErrAccessDenied
	}
	return open(ctx, req)
}
