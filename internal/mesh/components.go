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
			(binding.Spec.NeedsReadAccess && (binding.Prepare == nil || binding.Spec.Limits.MaxScanBytes == 0)) {
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
	if req.Operation == Invoke {
		binding, ok := l.tools[req.Tool.Tool]
		if !ok {
			return MeshReply{}, errors.Join(contract.ErrNotDelivered, contract.ErrBindingNotFound)
		}
		if req.Tool.RunID != id.run.RunID || req.Tool.CallID == "" || req.Tool.CallID != id.call.CallID ||
			req.Tool.Tool != id.call.Tool || req.Tool.Limits != id.call.Limits ||
			req.Tool.EnvironmentGrant != e.Grant || binding.Spec.NeedsReadAccess != (e.Grant != "") {
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
	switch req.Operation {
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
		grant, err := env.Grant(ctx, e.Call, e.Prefix)
		return MeshReply{Grant: grant}, err
	case OpenEnvironmentRead:
		reader, err := env.OpenRead(ctx, e.Call, e.Grant, e.Path)
		return MeshReply{Reader: reader}, err
	case RevokeEnvironment:
		scanned, err := env.Revoke(ctx, e.Call, e.Grant)
		return MeshReply{Scanned: scanned}, err
	case ReleaseEnvironment:
		return MeshReply{}, env.Release(ctx, e.Run, e.Handle)
	default:
		return MeshReply{}, contract.ErrInvalidRequest
	}
}

func CallComponent(ctx context.Context, transport MeshTransport, op Operation, env EnvironmentRequest, tool contract.ToolRequest) (MeshReply, error) {
	ctx = context.WithValue(ctx, componentKey{}, componentIdentity{run: env.Run, call: env.Call, ref: env.Ref})
	return transport.Call(ctx, MeshRequest{Operation: op, Environment: env, Tool: tool})
}
