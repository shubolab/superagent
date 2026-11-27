package mesh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"

	"github.com/shubolab/superagent/internal/contract"
)

func (l *Local) SetComponents(tools map[contract.ToolRef]contract.LocalToolBinding, environments map[contract.EnvironmentRef]contract.EnvironmentProvider) error {
	for ref, binding := range tools {
		if ref == "" || nilBinding(binding.Tool) || !contract.JSONObject(binding.Spec.InputSchema) ||
			binding.Spec.Limits.MaxResultBytes == 0 ||
			(binding.Spec.NeedsReadAccess && (binding.Prepare == nil || binding.Spec.Limits.MaxScanBytes == 0)) ||
			(binding.Spec.NeedsArtifactOutput && binding.Spec.Limits.MaxRawOutputBytes == 0) {
			return fmt.Errorf("%w: invalid tool %q", contract.ErrInvalidConfiguration, ref)
		}
	}
	for ref, provider := range environments {
		if ref == "" || nilBinding(provider) {
			return fmt.Errorf("%w: invalid execution environment %q", contract.ErrInvalidConfiguration, ref)
		}
	}
	l.tools, l.environments = maps.Clone(tools), maps.Clone(environments)
	return nil
}

type componentIdentity struct {
	run  contract.RunScope
	call contract.CallScope
	ref  contract.EnvironmentRef
}

type componentKey struct{}

func (l *Local) componentCall(ctx context.Context, req MeshRequest) (MeshReply, error) {
	id, _ := ctx.Value(componentKey{}).(componentIdentity)
	e := req.Environment
	if id.run.Subject == "" || id.run.RunID == "" || !l.HasRunner(id.run.Runner) ||
		e.Run != id.run || e.Call != id.call || e.Ref != id.ref {
		return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
	}
	if err := ctx.Err(); err != nil {
		return MeshReply{}, errors.Join(contract.ErrNotDelivered, err)
	}
	if req.Operation == BeginArtifactOutput || req.Operation == SettleArtifactOutput {
		return l.artifactCall(ctx, req.Operation, e)
	}
	if req.Operation == VerifyRetainedArtifact {
		return l.readArtifact(ctx, e.Run.Subject, MeshRequest{Runner: e.Run.Runner, RunID: e.Run.RunID, Artifact: req.Artifact})
	}
	if req.Operation == QueryReceipt {
		key := req.ReceiptKey
		if !validReceiptRoute(key, e) || key.Tool != e.Call.Tool || key.CallID != e.Call.CallID {
			return MeshReply{}, contract.ErrAccessDenied
		}
		provider, ok := l.tools[key.Tool].Tool.(contract.ReceiptProvider)
		if !ok {
			return MeshReply{}, contract.ErrRecoveryUnavailable
		}
		receipt, err := provider.QueryReceipt(ctx, key)
		return MeshReply{Receipt: receipt}, err
	}
	if req.Operation == Invoke {
		binding, ok := l.tools[req.Tool.Tool]
		if !ok {
			return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrBindingNotFound)
		}
		if req.Tool.RunID != id.run.RunID || req.Tool.CallID == "" || req.Tool.CallID != id.call.CallID ||
			req.Tool.Tool != id.call.Tool || req.Tool.Limits != id.call.Limits ||
			req.Tool.EnvironmentGrant != e.Grant || binding.Spec.NeedsReadAccess != (e.Grant != "") ||
			binding.Spec.NeedsArtifactOutput != (req.Tool.Limits.MaxRawOutputBytes > 0) {
			return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
		}
		toolCtx := contract.WithFileOpener(ctx, func(openCtx context.Context, claimed contract.ToolRequest, path string) (io.ReadCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if claimed.RunID != req.Tool.RunID || claimed.CallID != req.Tool.CallID ||
				claimed.Tool != req.Tool.Tool || claimed.EnvironmentGrant == "" ||
				claimed.EnvironmentGrant != e.Grant || claimed.Limits != req.Tool.Limits || path != id.call.Path {
				return nil, contract.ErrAccessDenied
			}
			// The Run context remains the owner even if the tool supplies a detached context.
			if err := openCtx.Err(); err != nil {
				return nil, err
			}
			read := e
			read.Path = path
			reply, err := l.Call(ctx, MeshRequest{Operation: OpenEnvironmentRead, Environment: read})
			return reply.Reader, err
		})
		if binding.Spec.NeedsArtifactOutput {
			toolCtx = l.withArtifactOutput(toolCtx, e, req.Tool)
		}
		if key := req.ReceiptKey; key != (contract.ReceiptKey{}) {
			if !validReceiptRoute(key, e) || key.Tool != req.Tool.Tool || key.CallID != req.Tool.CallID {
				return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
			}
			toolCtx = contract.WithReceiptKey(toolCtx, key)
		}
		result, err := binding.Tool.Invoke(toolCtx, req.Tool)
		if err != nil {
			// Delivery already happened. An error from the tool is not Mesh delivery evidence.
			return MeshReply{}, fmt.Errorf("tool invocation: %v", err)
		}
		return MeshReply{Tool: result}, nil
	}
	env, ok := l.environments[e.Ref]
	if !ok {
		return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrBindingNotFound)
	}
	if e.Call != (contract.CallScope{}) && e.Call.Run != e.Run {
		return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
	}
	// The workspace grant only governs read resources; artifact output has its own scope.
	scope := e.Call
	scope.Limits.MaxRawOutputBytes = 0

	switch req.Operation {
	case VerifyRecoveryWorkspace:
		verifier, ok := env.(contract.RecoveryWorkspace)
		if !ok {
			return MeshReply{}, contract.ErrRecoveryUnavailable
		}
		version, err := verifier.RecoveryWorkspace(ctx, e.Run)
		return MeshReply{WorkspaceVersion: version}, err
	case DescribeEnvironmentRead:
		descriptor, ok := env.(contract.EnvironmentReadDescriptor)
		if !ok {
			return MeshReply{}, nil
		}
		capabilities, err := descriptor.DescribeRead(ctx, e.Run.Workspace)
		return MeshReply{ReadCapabilities: &capabilities}, err
	case AllocateEnvironment:
		handle, err := env.Allocate(ctx, e.Run)
		return MeshReply{Handle: handle}, err
	case GrantEnvironment:
		grant, err := env.Grant(ctx, scope, e.Prefix)
		return MeshReply{Grant: grant}, err
	case OpenEnvironmentRead:
		reader, err := env.OpenRead(ctx, scope, e.Grant, e.Path)
		return MeshReply{Reader: reader}, err
	case RevokeEnvironment:
		scanned, err := env.Revoke(ctx, scope, e.Grant)
		return MeshReply{Scanned: scanned}, err
	case ReleaseEnvironment:
		return MeshReply{}, env.Release(ctx, e.Run, e.Handle)
	default:
		return MeshReply{}, contract.ErrInvalidRequest
	}
}

func CallComponent(ctx context.Context, transport MeshTransport, op Operation, env EnvironmentRequest, tool contract.ToolRequest) (MeshReply, error) {
	return callComponent(ctx, transport, MeshRequest{Operation: op, Environment: env, Tool: tool})
}

func CallReceipt(ctx context.Context, transport MeshTransport, op Operation, env EnvironmentRequest, tool contract.ToolRequest, key contract.ReceiptKey) (MeshReply, error) {
	return callComponent(ctx, transport, MeshRequest{Operation: op, Environment: env, Tool: tool, ReceiptKey: key})
}

func CheckRetainedArtifact(ctx context.Context, transport MeshTransport, env EnvironmentRequest, ref contract.ArtifactRef) error {
	_, err := callComponent(ctx, transport, MeshRequest{Operation: VerifyRetainedArtifact, Environment: env, Artifact: ArtifactRead{Ref: ref, MaxBytes: 1}})
	return err
}

func callComponent(ctx context.Context, transport MeshTransport, req MeshRequest) (MeshReply, error) {
	e := req.Environment
	ctx = context.WithValue(ctx, componentKey{}, componentIdentity{run: e.Run, call: e.Call, ref: e.Ref})
	return transport.Call(ctx, req)
}

func validReceiptRoute(key contract.ReceiptKey, e EnvironmentRequest) bool {
	return key.Runner == e.Run.Runner && key.Subject == e.Run.Subject && key.RunID == e.Run.RunID &&
		key.Step != 0 && key.Environment == e.Ref && key.Workspace == e.Run.Workspace &&
		key.BindingVersion != "" && key.Fingerprint != ""
}
