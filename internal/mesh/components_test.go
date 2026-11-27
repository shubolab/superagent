package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type componentTool func(context.Context, contract.ToolRequest) (contract.ToolResult, error)

func (f componentTool) Invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	return f(ctx, req)
}

type routeEnvironment struct {
	t        *testing.T
	executed []Operation
	run      contract.RunScope
	call     contract.CallScope
}

func (e *routeEnvironment) checkRun(ctx context.Context, op Operation, scope contract.RunScope) {
	e.t.Helper()
	if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(e.run.Deadline) || scope != e.run {
		e.t.Fatalf("routed scope %+v, deadline %v", scope, deadline)
	}
	e.executed = append(e.executed, op)
}

func (e *routeEnvironment) checkCall(ctx context.Context, op Operation, scope contract.CallScope) {
	e.t.Helper()
	e.checkRun(ctx, op, scope.Run)
	if scope != e.call {
		e.t.Fatalf("routed call %+v, want %+v", scope, e.call)
	}
}

func (e *routeEnvironment) Allocate(ctx context.Context, s contract.RunScope) (contract.EnvironmentHandle, error) {
	e.checkRun(ctx, AllocateEnvironment, s)
	return e.call.Handle, nil
}

func (e *routeEnvironment) Grant(ctx context.Context, s contract.CallScope, prefix string) (contract.EnvironmentGrantRef, error) {
	e.checkCall(ctx, GrantEnvironment, s)
	if prefix != "docs" {
		e.t.Fatalf("wrong prefix %q", prefix)
	}
	return "grant", nil
}

func (e *routeEnvironment) OpenRead(ctx context.Context, s contract.CallScope, ref contract.EnvironmentGrantRef, path string) (io.ReadCloser, error) {
	e.checkCall(ctx, OpenEnvironmentRead, s)
	if ref != "grant" || path != e.call.Path {
		e.t.Fatalf("wrong open %q, %q", ref, path)
	}
	return io.NopCloser(strings.NewReader("ok")), nil
}

func (e *routeEnvironment) Revoke(ctx context.Context, s contract.CallScope, ref contract.EnvironmentGrantRef) (uint64, error) {
	e.checkCall(ctx, RevokeEnvironment, s)
	if ref != "grant" {
		e.t.Fatalf("wrong revoke %q", ref)
	}
	return 2, nil
}

func (e *routeEnvironment) Release(ctx context.Context, s contract.RunScope, handle contract.EnvironmentHandle) error {
	e.checkRun(ctx, ReleaseEnvironment, s)
	if handle != e.call.Handle {
		e.t.Fatalf("wrong release %q", handle)
	}
	return nil
}

func TestLocalRoutesTrustedToolAndGrant(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	run := contract.RunScope{Runner: "r", Subject: "alice", RunID: "run", Workspace: "w", Deadline: deadline}
	limits := contract.ToolLimits{MaxScanBytes: 32, MaxResultBytes: 4096}
	call := contract.CallScope{Run: run, CallID: "c1", Handle: "h1", Tool: "search", Path: "docs/a", Limits: limits}
	env := &routeEnvironment{t: t, run: run, call: call}
	l, err := NewLocal(map[contract.RunnerRef]RunnerEndpoint{"r": &endpointStub{}},
		map[contract.ModelRef]contract.ModelProvider{"m": providerFunc(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
			t.Fatal("unexpected model request")
			return nil, nil
		})}, func(context.Context) (contract.SubjectID, error) { return "wrong-for-components", nil })
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	tool := componentTool(func(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
		n++
		if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) || req.RunID != run.RunID ||
			req.CallID != call.CallID || req.Tool != call.Tool || req.EnvironmentGrant != "grant" || req.Limits != limits {
			t.Fatalf("incorrect tool request: %+v, deadline %v", req, d)
		}
		bad := req
		bad.CallID = "foreign"
		if _, err := contract.OpenToolFile(ctx, bad, call.Path); !errors.Is(err, contract.ErrAccessDenied) {
			t.Fatalf("foreign read: %v", err)
		}
		r, err := contract.OpenToolFile(ctx, req, call.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if string(data) != "ok" || err != nil {
			t.Fatalf("open data %q, %v", data, err)
		}
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID}, nil
	})
	if err := l.SetComponents(map[contract.ToolRef]contract.LocalToolBinding{"search": {
		Spec: contract.ToolSpec{NeedsReadAccess: true, InputSchema: json.RawMessage(`{}`), Limits: limits},
		Tool: tool, Prepare: func(json.RawMessage) (string, error) { return call.Path, nil },
	}}, map[contract.EnvironmentRef]contract.EnvironmentProvider{"local": env}); err != nil {
		t.Fatal(err)
	}

	e := EnvironmentRequest{Ref: "local", Run: run}
	empty := contract.ToolRequest{}
	routed := func(op Operation, req contract.ToolRequest) MeshReply {
		reply, err := CallComponent(ctx, l, op, e, req)
		if err != nil {
			t.Fatalf("operation %d: %v", op, err)
		}
		return reply
	}
	e.Handle = routed(AllocateEnvironment, empty).Handle
	e.Call, e.Prefix = call, "docs"
	e.Grant = routed(GrantEnvironment, empty).Grant
	req := contract.ToolRequest{RunID: run.RunID, CallID: call.CallID, Tool: call.Tool, Limits: limits, EnvironmentGrant: e.Grant}
	if _, err := l.Call(ctx, MeshRequest{Operation: Invoke, Environment: e, Tool: req}); !errors.Is(err, contract.ErrNotDelivered) {
		t.Fatalf("untrusted invoke error = %v", err)
	}
	for _, bad := range []contract.ToolRequest{
		{Tool: "missing"},
		{Tool: call.Tool, RunID: "other", CallID: req.CallID, Limits: limits, EnvironmentGrant: e.Grant},
	} {
		if _, err := CallComponent(ctx, l, Invoke, e, bad); !errors.Is(err, contract.ErrNotDelivered) {
			t.Fatalf("mismatched invoke error = %v", err)
		}
	}
	unknownEnv := e
	unknownEnv.Ref = "missing"
	if _, err := CallComponent(ctx, l, AllocateEnvironment, unknownEnv, empty); !errors.Is(err, contract.ErrBindingNotFound) {
		t.Fatalf("unknown environment error = %v", err)
	}
	if reply := routed(Invoke, req); reply.Tool.RunID != run.RunID || reply.Tool.CallID != call.CallID {
		t.Fatalf("lost reply correlation: %+v", reply)
	}
	if reply := routed(RevokeEnvironment, empty); reply.Scanned != 2 {
		t.Fatalf("lost meter: %+v", reply)
	}
	e.Call = contract.CallScope{}
	routed(ReleaseEnvironment, empty)
	want := []Operation{AllocateEnvironment, GrantEnvironment, OpenEnvironmentRead, RevokeEnvironment, ReleaseEnvironment}
	if n != 1 || !reflect.DeepEqual(env.executed, want) {
		t.Fatalf("Invoke = %d, operations = %v", n, env.executed)
	}
}
