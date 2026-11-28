package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

func (l *Local) SetArtifactStore(store contract.ArtifactStore) error {
	if store != nil && nilBinding(store) {
		return contract.ErrInvalidConfiguration
	}
	if durable, ok := store.(contract.DurableArtifactStore); ok &&
		(durable.ArtifactStoreID() == "" || !durable.RetainsAcrossRestart() || !durable.StagesUntilPublication()) {
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

func (l *Local) artifactCall(ctx context.Context, req MeshRequest) (MeshReply, error) {
	e, key := req.Environment, req.ReceiptKey
	if l.artifacts == nil || e.Call.Run != e.Run || e.Call.CallID == "" {
		return MeshReply{}, contract.ErrArtifactUnavailable
	}
	scope := artifactScope(e)
	store, durable := l.artifacts.(contract.DurableArtifactStore)
	lease, trusted := contract.StorageRun(ctx, e.Run)
	if (durable && !trusted) || (e.Run.Epoch != 0 && !durable) {
		return MeshReply{}, contract.ErrAccessDenied
	}
	if durable && len(e.Artifacts) > 1 {
		return MeshReply{}, contract.ErrArtifactReceipt
	}
	admission := contract.ArtifactAdmission{Call: scope, Deadline: e.Run.Deadline, Epoch: e.Run.Epoch, Key: key}
	if req.Operation == BeginArtifactOutput {
		admission.Operation = contract.ArtifactReserve
		reserveCtx, end := contract.WithScopedArtifactAdmission(ctx, admission)
		defer end()
		limit, err := l.artifacts.Begin(reserveCtx, scope)
		return MeshReply{RawOutputBytes: limit}, err
	}
	restoring := req.Operation == RestoreArtifactOutput
	if restoring {
		if !durable || len(e.Artifacts) != 1 || !validReceiptRoute(key, e) || key.CallID != scope.CallID || key.Tool != scope.Tool {
			return MeshReply{}, contract.ErrAccessDenied
		}
		query := req
		query.Operation = QueryReceipt
		if err := admitComponent(ctx, query); err != nil {
			return MeshReply{}, err
		}
	}
	publish := func(ctx context.Context) (uint64, error) { return l.artifacts.Settle(ctx, scope, e.Artifacts) }
	admission.Operation = contract.ArtifactDiscard
	if durable && len(e.Artifacts) == 1 {
		admission.Operation, admission.Ref = contract.ArtifactPreparePublication, e.Artifacts[0]
		prepareCtx, end := contract.WithScopedArtifactAdmission(ctx, admission)
		var err error
		publish, err = store.PrepareArtifactPublication(prepareCtx, scope, admission.Ref)
		end()
		if err != nil {
			return MeshReply{}, err
		}
		if publish == nil {
			return MeshReply{}, contract.ErrArtifactReceipt
		}
		admission.Operation = contract.ArtifactPublish
	}
	var written uint64
	commit := func() error {
		commitCtx, end := contract.WithScopedArtifactAdmission(ctx, admission)
		defer end()
		if _, err := contract.VerifyArtifactAccess(commitCtx, admission.Operation, scope, admission.Ref); err != nil {
			return err
		}
		var err error
		written, err = publish(commitCtx)
		return err
	}
	if e.Run.Epoch == 0 {
		err := commit()
		return MeshReply{RawOutputBytes: written}, err
	}
	if restoring {
		err := lease.RecoverPublication(ctx, e.Run, key, commit)
		return MeshReply{RawOutputBytes: written}, err
	}
	err := lease.Publish(ctx, e.Run, key, commit)
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
		if _, durable := l.artifacts.(contract.DurableArtifactStore); durable {
			lease, trusted := contract.StorageRun(ctx, e.Run)
			if !trusted {
				return nil, contract.ErrAccessDenied
			}
			scope := artifactScope(e)
			admission := contract.ArtifactAdmission{Operation: contract.ArtifactOpenWriter, Call: scope,
				Deadline: e.Run.Deadline, Epoch: e.Run.Epoch, Key: key, Active: func() bool { return ctx.Err() == nil }}
			if lease != nil {
				admission.Seal = func(sealCtx context.Context, seal func() error) error {
					return lease.Publish(sealCtx, e.Run, key, seal)
				}
			}
			return l.artifacts.OpenWriter(contract.WithArtifactWriterAdmission(ctx, admission), scope)
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
	readCtx, end := contract.WithScopedArtifactAdmission(ctx, contract.ArtifactAdmission{
		Operation: contract.ArtifactRead, Call: contract.ArtifactCall{Owner: owner}, Ref: req.Artifact.Ref,
	})
	defer end()
	page, err := l.artifacts.ReadArtifact(readCtx, owner, req.Artifact.Ref, req.Artifact.Offset, req.Artifact.MaxBytes)
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
