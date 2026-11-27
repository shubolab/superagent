//go:build darwin || linux

package environment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestLocalReadAdmissionBoundsAndReturnsSlots(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("bounded"), 0600); err != nil {
		t.Fatal(err)
	}
	local, err := NewLocalWithOptions(map[contract.WorkspaceRef]WorkspaceConfig{"shared": {Root: dir, ReadPrefixes: []string{"."}}},
		ReadOptions{MaxScanBytes: 7, MaxConcurrentReads: 1, MaxQueuedReads: 1})
	if err != nil {
		t.Fatal(err)
	}
	makeCall := func(id contract.RunID) contract.CallScope {
		t.Helper()
		run := contract.RunScope{Runner: "r", Subject: "s", RunID: id, Workspace: "shared", Deadline: time.Now().Add(time.Minute)}
		handle, err := local.Allocate(ctx, run)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = local.Release(context.Background(), run, handle) })
		return contract.CallScope{Run: run, CallID: "inspect", Tool: "read", Handle: handle, Path: "file",
			Limits: contract.ToolLimits{MaxScanBytes: 7, MaxResultBytes: 128}}
	}
	first, second, third := makeCall("first"), makeCall("second"), makeCall("third")
	excess := first
	excess.Limits.MaxScanBytes++
	if _, err := local.Grant(ctx, excess, "."); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("over-cap grant = %v", err)
	}
	occupant, err := local.Grant(ctx, first, ".")
	if err != nil {
		t.Fatal(err)
	}

	type reply struct {
		grant contract.EnvironmentGrantRef
		err   error
	}
	waitForSlot := func(ctx context.Context, scope contract.CallScope) <-chan reply {
		result := make(chan reply, 1)
		go func() {
			grant, err := local.Grant(ctx, scope, ".")
			result <- reply{grant: grant, err: err}
		}()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for len(local.admission.waiting) != 1 {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("request never occupied the queue")
			}
		}
		return result
	}
	queuedCtx, stopQueued := context.WithCancel(ctx)
	defer stopQueued()
	queued := waitForSlot(queuedCtx, second)
	if _, err := local.Grant(ctx, third, "."); !errors.Is(err, contract.ErrEnvironmentBusy) {
		t.Fatalf("third concurrent grant = %v", err)
	}
	stopQueued()
	select {
	case got := <-queued:
		if got.grant != "" || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancelled queued grant = %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	next := waitForSlot(ctx, third)
	if _, err := local.Revoke(ctx, first, occupant); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-next:
		if got.grant == "" || got.err != nil {
			t.Fatalf("queued grant after revoke = %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := local.Release(ctx, third.Run, third.Handle); err != nil {
		t.Fatal(err)
	}
	if len(local.admission.active) != 0 || len(local.admission.waiting) != 0 {
		t.Fatal("release did not return all read capacity")
	}
}
