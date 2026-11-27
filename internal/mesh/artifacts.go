package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

func (l *Local) SetArtifactStore(store contract.ArtifactStore) error {
	if store != nil && nilBinding(store) {
		return contract.ErrInvalidConfiguration
	}
	l.artifacts = store
	return nil
}

func artifactScope(e EnvironmentRequest) contract.ArtifactCall {
	return contract.ArtifactCall{
		Owner:  contract.ArtifactOwner{Runner: e.Run.Runner, Subject: e.Run.Subject, RunID: e.Run.RunID},
		CallID: e.Call.CallID, Tool: e.Call.Tool, MaxBytes: e.Call.Limits.MaxRawOutputBytes,
	}
}

func (l *Local) artifactCall(ctx context.Context, op Operation, e EnvironmentRequest) (MeshReply, error) {
	if l.artifacts == nil || e.Call.Run != e.Run || e.Call.CallID == "" {
		return MeshReply{}, contract.ErrArtifactUnavailable
	}
	scope := artifactScope(e)
	if op == BeginArtifactOutput {
		limit, err := l.artifacts.Begin(ctx, scope)
		return MeshReply{RawOutputBytes: limit}, err
	}
	written, err := l.artifacts.Settle(ctx, scope, e.Artifacts)
	return MeshReply{RawOutputBytes: written}, err
}

func (l *Local) withArtifactOutput(ctx context.Context, e EnvironmentRequest, req contract.ToolRequest, key contract.ReceiptKey) context.Context {
	return contract.WithArtifactOpener(ctx, func(callCtx context.Context, claimed contract.ToolRequest) (contract.ArtifactWriter, error) {
		if err := callCtx.Err(); err != nil {
			return nil, err
		}
		if l.artifacts == nil || claimed.RunID != req.RunID || claimed.CallID != req.CallID ||
			claimed.Tool != req.Tool || claimed.Limits != req.Limits || claimed.EnvironmentGrant != req.EnvironmentGrant {
			return nil, contract.ErrAccessDenied
		}
		writer, err := l.artifacts.OpenWriter(ctx, artifactScope(e))
		if err != nil || e.Run.Epoch == 0 {
			return writer, err
		}
		guard := takeoverGuard(ctx)
		if guard == nil {
			_ = writer.Close()
			return nil, contract.ErrAccessDenied
		}
		return &fencedArtifact{ArtifactWriter: writer, guard: guard, ctx: ctx, scope: e.Run, key: key}, nil
	})
}

func (l *Local) readArtifact(ctx context.Context, subject contract.SubjectID, req MeshRequest) (MeshReply, error) {
	if l.artifacts == nil {
		return MeshReply{}, contract.ErrArtifactUnavailable
	}
	owner := contract.ArtifactOwner{Runner: req.Runner, Subject: subject, RunID: req.RunID}
	page, err := l.artifacts.ReadArtifact(ctx, owner, req.Artifact.Ref, req.Artifact.Offset, req.Artifact.MaxBytes)
	return MeshReply{ArtifactPage: page}, err
}

type fencedArtifact struct {
	contract.ArtifactWriter
	guard TakeoverGuard
	ctx   context.Context
	scope contract.RunScope
	key   contract.ReceiptKey
}

func (w *fencedArtifact) Complete() (contract.ArtifactRef, error) {
	var ref contract.ArtifactRef
	err := w.guard.Publish(w.ctx, w.scope, w.key, func() error {
		var err error
		ref, err = w.ArtifactWriter.Complete()
		return err
	})
	return ref, err
}
