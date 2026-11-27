//go:build darwin || linux

package superagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/shubolab/superagent"
)

func capacityFixture(t *testing.T) (*searchFixture, superagent.EnvironmentProvider) {
	t.Helper()
	f := newSearchFixture(t)
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docs", "health.txt"), []byte("status=ready\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := superagent.NewLocalReadEnvironmentWithOptions(
		map[superagent.WorkspaceRef]superagent.WorkspaceConfig{"workspace": {Root: dir, ReadPrefixes: []string{"docs"}}},
		superagent.LocalReadEnvironmentOptions{MaxScanBytes: 7, MaxConcurrentReads: 1, MaxQueuedReads: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.LocalEnvironments["local"] = env
	return f, env
}

func TestLocalReadCapacityAcrossRuns(t *testing.T) {
	ctx := hostContext(t, "alice")
	f, env := capacityFixture(t)
	descriptor, ok := env.(superagent.EnvironmentReadDescriptor)
	if !ok {
		t.Fatal("local environment does not describe actual capacity")
	}
	caps, err := descriptor.DescribeRead(ctx, "workspace")
	if err != nil || caps.Scan != superagent.CapabilityEnforced || caps.Deadline != superagent.CapabilityEnforced ||
		caps.MaxScanBytes != 7 || caps.MaxConcurrentReads != 1 || caps.MaxQueuedReads != 1 {
		t.Fatalf("reported capacity %+v, error %v", caps, err)
	}
	f.cfg.LocalModels["chat"] = modelFunc(func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if req.Step == 1 {
			return scripted(searchCompleted("", searchCall("read", "docs/health.txt"))), nil
		}
		return scripted(completed("done")), nil
	})
	dispatched := make(chan superagent.ToolRequest, 4)
	unblock := make(chan struct{})
	tool := f.cfg.LocalTools["search"]
	original := tool.Tool
	tool.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		dispatched <- req
		if req.RunID == "active" {
			select {
			case <-unblock:
			case <-ctx.Done():
				return superagent.ToolResult{}, ctx.Err()
			}
		}
		return original.Invoke(ctx, req)
	})
	f.cfg.LocalTools["search"] = tool
	client := newClient(t, f.cfg)
	req := f.request()
	req.ID = "active"
	first := start(t, ctx, client, req)
	defer first.Cancel(ctx)
	if tool := recv(t, ctx, dispatched); tool.RunID != "active" || tool.Limits.MaxScanBytes != 7 {
		t.Fatalf("incorrect granted limits %+v", tool)
	}
	req.ID = "second"
	second := start(t, ctx, client, req)
	defer second.Cancel(ctx)
	req.ID = "third"
	third := start(t, ctx, client, req)
	defer third.Cancel(ctx)

	type finished struct {
		run    *superagent.Run
		result superagent.Result
		err    error
	}
	finishedRuns := make(chan finished, 2)
	for _, run := range []*superagent.Run{second, third} {
		go func() {
			result, err := run.Wait(ctx)
			finishedRuns <- finished{run: run, result: result, err: err}
		}()
	}
	overloaded := recv(t, ctx, finishedRuns)
	if overloaded.err != nil || overloaded.result.Outcome != superagent.Succeeded || len(overloaded.result.Messages) != 4 {
		t.Fatalf("overload did not return a paired rejection: %+v, %v", overloaded.result, overloaded.err)
	}
	result := overloaded.result.Messages[2].Tool.Result
	if result.State != superagent.ToolExecutionNotExecuted || result.Reason.Code != "environment_busy" {
		t.Fatalf("overload receipt %+v", result)
	}
	found := false
	for _, event := range allRunEvents(t, observeRun(t, ctx, overloaded.run)) {
		if event.Kind == superagent.EventToolDispatched {
			t.Fatal("overloaded tool was dispatched")
		}
		if event.Kind == superagent.EventToolCompleted && event.Tool.Status == superagent.ToolStatusRejected && event.Tool.Code == "environment_busy" {
			found = true
		}
	}
	if !found || len(dispatched) != 0 {
		t.Fatal("pre-dispatch rejection was not observed")
	}

	queued := second
	if overloaded.run.ID() == queued.ID() {
		queued = third
	}
	if err := queued.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	cancelled := recv(t, ctx, finishedRuns)
	if cancelled.err != nil || cancelled.run.ID() != queued.ID() || cancelled.result.Outcome != superagent.Cancelled {
		t.Fatalf("queued cancellation %+v, %v", cancelled.result, cancelled.err)
	}
	close(unblock)
	active := wait(t, ctx, first, superagent.Succeeded)
	read := active.Messages[2].Tool.Result
	var report superagent.SearchReport
	if read.Usage == nil || read.Usage.ScannedBytes == nil || *read.Usage.ScannedBytes != 7 ||
		json.Unmarshal([]byte(read.Completion.Content), &report) != nil || report.Coverage != "partial" || report.Reason != "scan_limit" {
		t.Fatalf("environment limit not enforced: %+v, report %+v", read, report)
	}
	req.ID = "next"
	next := start(t, ctx, client, req)
	if got := recv(t, ctx, dispatched); got.RunID != "next" || got.Limits.MaxScanBytes != 7 {
		t.Fatalf("capacity was not reusable: %+v", got)
	}
	wait(t, ctx, next, superagent.Succeeded)
}

type insufficientReadDescriptor struct{ superagent.EnvironmentProvider }

func (insufficientReadDescriptor) DescribeRead(context.Context, superagent.WorkspaceRef) (superagent.EnvironmentReadCapabilities, error) {
	return superagent.EnvironmentReadCapabilities{Scan: superagent.CapabilityMeasured, Deadline: superagent.CapabilityEnforced,
		MaxScanBytes: 7, MaxConcurrentReads: 1}, nil
}

func TestReadCapabilityMismatchStopsBeforeDispatch(t *testing.T) {
	ctx := hostContext(t, "alice")
	f, env := capacityFixture(t)
	f.cfg.LocalEnvironments["local"] = insufficientReadDescriptor{env}
	r := wait(t, ctx, start(t, ctx, newClient(t, f.cfg), f.request()), superagent.Failed)
	if r.Stop.Code != superagent.StopEnvironmentCapability || len(f.calls) != 0 || f.hits.Load() != 0 {
		t.Fatalf("unexpected capability failure %+v", r.Stop)
	}
	_, err := superagent.NewLocalReadEnvironmentWithOptions(nil, superagent.LocalReadEnvironmentOptions{})
	if !errors.Is(err, superagent.ErrInvalidConfiguration) {
		t.Fatalf("invalid capacity error = %v", err)
	}
}
