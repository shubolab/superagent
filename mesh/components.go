package mesh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/remotetool"
)

func (l *Local) SetComponents(tools map[contract.ToolRef]contract.LocalToolBinding, environments map[contract.EnvironmentRef]contract.EnvironmentProvider) error {
	for ref, binding := range tools {
		if remote, ok := binding.Tool.(*remotetool.Client); ok && remote != nil && remote.Identity().EvidenceRule != "" && binding.Prepare == nil {
			return fmt.Errorf("%w: write evidence tool %q requires argument preparation", contract.ErrInvalidConfiguration, ref)
		}
		if ref == "" || nilBinding(binding.Tool) || !contract.JSONObject(binding.Spec.InputSchema) ||
			binding.Spec.Limits.MaxResultBytes == 0 ||
			(binding.Spec.NeedsWorkspaceWrite && !binding.Spec.NeedsReadAccess) ||
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
	if req.Operation == BeginArtifactOutput {
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, err
		}
	}
	if req.Operation == BeginArtifactOutput || req.Operation == SettleArtifactOutput || req.Operation == RestoreArtifactOutput {
		return l.artifactCall(ctx, req)
	}
	if req.Operation == VerifyRetainedArtifact {
		store, ok := l.artifacts.(contract.DurableArtifactStore)
		if !ok {
			return MeshReply{}, contract.ErrArtifactUnavailable
		}
		if _, trusted := contract.StorageRun(ctx, e.Run); !trusted {
			return MeshReply{}, contract.ErrAccessDenied
		}
		owner := artifactScope(e).Owner
		verifyCtx, end := contract.WithScopedArtifactAdmission(ctx, contract.ArtifactAdmission{
			Operation: contract.ArtifactCheck, Call: contract.ArtifactCall{Owner: owner}, Ref: req.Artifact.Ref,
			Deadline: e.Run.Deadline, Epoch: e.Run.Epoch,
		})
		defer end()
		return MeshReply{}, store.CheckArtifact(verifyCtx, owner, req.Artifact.Ref)
	}
	if req.Operation == VerifyRemoteToolWorkspace {
		key := req.ReceiptKey
		if key.CallID != "" || key.Subject != e.Run.Subject || key.Runner != e.Run.Runner || key.RunID != e.Run.RunID {
			return MeshReply{}, contract.ErrAccessDenied
		}
		remote, ok := l.tools[key.Tool].Tool.(*remotetool.Client)
		if !ok || key.Remote != remote.Identity() {
			return MeshReply{}, contract.ErrRecoveryBinding
		}
		owner, err := remoteOwner(ctx, e.Run)
		if err != nil {
			return MeshReply{}, err
		}
		return MeshReply{}, remote.VerifyWorkspace(ctx, key, e.Run, owner)
	}
	if req.Operation == QueryReceipt {
		key := req.ReceiptKey
		if !validReceiptRoute(key, e) || key.Tool != e.Call.Tool || key.CallID != e.Call.CallID {
			return MeshReply{}, contract.ErrAccessDenied
		}
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, err
		}
		if remote, ok := l.tools[key.Tool].Tool.(*remotetool.Client); ok {
			owner, err := remoteOwner(ctx, e.Run)
			if err != nil {
				return MeshReply{}, err
			}
			receipt, err := remote.QueryRemote(ctx, key, e.Run, owner, req.Tool.Limits)
			return MeshReply{Receipt: receipt}, err
		}
		if key.Remote != (contract.RemoteToolIdentity{}) {
			return MeshReply{}, contract.ErrRecoveryBinding
		}
		provider, ok := l.tools[key.Tool].Tool.(contract.ReceiptProvider)
		if !ok {
			return MeshReply{}, contract.ErrRecoveryUnavailable
		}
		receipt, err := provider.QueryReceipt(contract.WithoutStorageRun(ctx), key)
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
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, errors.Join(contract.ErrNotDelivered, err)
		}
		if remote, ok := binding.Tool.(*remotetool.Client); ok {
			key := req.ReceiptKey
			if !validReceiptRoute(key, e) || key.Remote != remote.Identity() ||
				key.Tool != req.Tool.Tool || key.CallID != req.Tool.CallID {
				return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
			}
			owner, err := remoteOwner(ctx, e.Run)
			if err != nil {
				return MeshReply{}, errors.Join(contract.ErrNotDelivered, err)
			}
			receipt, err := remote.InvokeRemote(ctx, key, e.Run, owner, req.Tool)
			if errors.Is(err, remotetool.ErrResponseLost) {
				return MeshReply{}, errors.Join(ErrResponseLost, err)
			}
			if err != nil {
				return MeshReply{}, err
			}
			return MeshReply{Tool: receipt.Result, Cleanup: receipt.Cleanup}, nil
		}
		if req.ReceiptKey.Remote != (contract.RemoteToolIdentity{}) {
			return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrRecoveryBinding)
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
			artifactCtx, end := context.WithCancel(toolCtx)
			defer end()
			toolCtx = l.withArtifactOutput(artifactCtx, e, req.Tool, req.ReceiptKey)
		}
		if binding.Spec.NeedsWorkspaceWrite {
			toolCtx = l.withWorkspacePublisher(toolCtx, e, req.Tool, req.ReceiptKey)
		}
		if key := req.ReceiptKey; key != (contract.ReceiptKey{}) {
			if !validReceiptRoute(key, e) || key.Tool != req.Tool.Tool || key.CallID != req.Tool.CallID {
				return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrAccessDenied)
			}
			toolCtx = contract.WithReceiptKey(toolCtx, key)
		}
		result, err := binding.Tool.Invoke(contract.WithoutStorageRun(toolCtx), req.Tool)
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
	envCtx := contract.WithoutStorageRun(ctx)

	switch req.Operation {
	case VerifyRecoveryWorkspace:
		if e.Run.Epoch != 0 {
			fencer, ok := env.(contract.TakeoverWorkspace)
			if !ok || !fencer.FencesWorkspace(e.Run.Workspace) {
				return MeshReply{}, contract.ErrRecoveryUnavailable
			}
		}
		verifier, ok := env.(contract.RecoveryWorkspace)
		if !ok {
			return MeshReply{}, contract.ErrRecoveryUnavailable
		}
		version, err := verifier.RecoveryWorkspace(envCtx, e.Run)
		return MeshReply{WorkspaceVersion: version}, err
	case DescribeEnvironmentRead:
		descriptor, ok := env.(contract.EnvironmentReadDescriptor)
		if !ok {
			return MeshReply{}, nil
		}
		capabilities, err := descriptor.DescribeRead(envCtx, e.Run.Workspace)
		return MeshReply{ReadCapabilities: &capabilities}, err
	case AllocateEnvironment:
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, err
		}
		handle, err := env.Allocate(envCtx, e.Run)
		return MeshReply{Handle: handle}, err
	case GrantEnvironment:
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, err
		}
		grant, err := env.Grant(envCtx, scope, e.Prefix)
		if err == nil && e.Run.Epoch != 0 {
			if guardErr := admitComponent(ctx, req); guardErr != nil {
				_, _ = env.Revoke(context.WithoutCancel(envCtx), scope, grant)
				return MeshReply{}, guardErr
			}
		}
		return MeshReply{Grant: grant}, err
	case OpenEnvironmentRead:
		if err := admitComponent(ctx, req); err != nil {
			return MeshReply{}, err
		}
		reader, err := env.OpenRead(envCtx, scope, e.Grant, e.Path)
		return MeshReply{Reader: reader}, err
	case RevokeEnvironment:
		if e.Run.Epoch != 0 && !matchesTakeoverScope(ctx, e.Run) {
			return MeshReply{}, contract.ErrAccessDenied
		}
		scanned, err := env.Revoke(envCtx, scope, e.Grant)
		return MeshReply{Scanned: scanned}, err
	case ReleaseEnvironment:
		if e.Run.Epoch != 0 && !matchesTakeoverScope(ctx, e.Run) {
			return MeshReply{}, contract.ErrAccessDenied
		}
		return MeshReply{}, env.Release(envCtx, e.Run, e.Handle)
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
	_, err := callComponent(ctx, transport, MeshRequest{Operation: VerifyRetainedArtifact, Environment: env, Artifact: ArtifactRead{Ref: ref}})
	return err
}

func callComponent(ctx context.Context, transport MeshTransport, req MeshRequest) (MeshReply, error) {
	e := req.Environment
	ctx = context.WithValue(ctx, componentKey{}, componentIdentity{run: e.Run, call: e.Call, ref: e.Ref})
	return transport.Call(ctx, req)
}

func validReceiptRoute(key contract.ReceiptKey, e EnvironmentRequest) bool {
	return key.Runner == e.Run.Runner && key.Subject == e.Run.Subject && key.RunID == e.Run.RunID &&
		key.Step != 0 && (key.AdmissionEpoch == 0) == (e.Run.Epoch == 0) && key.Environment == e.Ref && key.Workspace == e.Run.Workspace &&
		key.BindingVersion != "" && key.Fingerprint != ""
}

func remoteOwner(ctx context.Context, scope contract.RunScope) (contract.RemoteOwner, error) {
	if scope.Epoch == 0 {
		return contract.RemoteOwner{}, nil
	}
	guard := takeoverGuard(ctx)
	if guard == nil || !guard.Matches(scope) {
		return contract.RemoteOwner{}, contract.ErrAccessDenied
	}
	return guard.RemoteOwner(), nil
}
