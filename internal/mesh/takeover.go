package mesh

import (
	"bytes"
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

type TakeoverGuard interface {
	Admit(context.Context, MeshRequest) error
	Matches(contract.RunScope) bool
	Publish(context.Context, contract.RunScope, contract.ReceiptKey, func() error) error
}

type takeoverGuardKey struct{}

func WithTakeoverGuard(ctx context.Context, guard TakeoverGuard) context.Context {
	return context.WithValue(ctx, takeoverGuardKey{}, guard)
}

func takeoverGuard(ctx context.Context) TakeoverGuard {
	guard, _ := ctx.Value(takeoverGuardKey{}).(TakeoverGuard)
	return guard
}

func admitComponent(ctx context.Context, req MeshRequest) error {
	guard := takeoverGuard(ctx)
	if req.Environment.Run.Epoch == 0 && guard == nil {
		return nil
	}
	if guard == nil || req.Environment.Run.Epoch == 0 {
		return contract.ErrAccessDenied
	}
	return guard.Admit(ctx, req)
}

func (l *Local) withWorkspacePublisher(ctx context.Context, e EnvironmentRequest, req contract.ToolRequest, key contract.ReceiptKey) context.Context {
	env, ok := l.environments[e.Ref].(contract.TakeoverWorkspace)
	guard := takeoverGuard(ctx)
	if !ok || guard == nil || key.AdmissionEpoch == 0 {
		return ctx
	}
	return contract.WithWorkspacePublisher(ctx, func(callCtx context.Context, claimed contract.ToolRequest, path string, data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if claimed.RunID != req.RunID || claimed.CallID != req.CallID || claimed.Tool != req.Tool ||
			claimed.EnvironmentGrant != e.Grant || claimed.Limits != req.Limits || path != e.Call.Path ||
			len(data) == 0 || len(data) > contract.MaxWorkspacePublishBytes {
			return contract.ErrAccessDenied
		}
		staged := bytes.Clone(data)
		scope := e.Call
		scope.Limits.MaxRawOutputBytes = 0
		return guard.Publish(callCtx, e.Run, key, func() error {
			version, err := env.RecoveryWorkspace(ctx, e.Run)
			if err != nil || version != key.WorkspaceVersion || !env.FencesWorkspace(e.Run.Workspace) {
				return contract.ErrAccessDenied
			}
			return env.PublishWorkspace(contract.WithWorkspacePublication(ctx, scope, e.Grant), scope, e.Grant, staged)
		})
	})
}

func matchesTakeoverScope(ctx context.Context, scope contract.RunScope) bool {
	guard := takeoverGuard(ctx)
	return guard != nil && guard.Matches(scope)
}
