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
