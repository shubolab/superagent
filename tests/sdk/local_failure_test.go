//go:build darwin || linux

package superagent_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shubolab/superagent"
)

func TestSavedFileWithLostToolResponse(t *testing.T) {
	f := newSearchFixture(t)
	ctx := hostContext(t, "alice")
	effects := filepath.Join(t.TempDir(), "saved.txt")
	change := f.cfg.LocalTools["search"]
	original := change.Tool
	change.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		if req.CallID == "first" {
			return original.Invoke(ctx, req)
		}
		f.hits.Add(1)
		file, err := os.OpenFile(effects, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return superagent.ToolResult{}, err
		}
		_, err = io.WriteString(file, "saved\n")
		if err := errors.Join(err, file.Close()); err != nil {
			return superagent.ToolResult{}, err
		}
		return superagent.ToolResult{}, errors.New("receipt lost after saving")
	})
	f.cfg.LocalTools["search"] = change
	req := f.request()
	req.Budget.MaxToolRequests = pointer(uint64(3))
	run := start(t, ctx, newClient(t, f.cfg), req)
	f.first.events <- searchCompleted("", searchCall("first", "docs/health.txt"), searchCall("save", "docs/health.txt"), searchCall("skipped", "docs/health.txt"))
	result := wait(t, ctx, run, superagent.ToolOutcomeUnknown)
	data, err := os.ReadFile(effects)
	if err != nil || string(data) != "saved\n" || f.hits.Load() != 2 || len(f.calls) != 1 ||
		len(result.Messages) != 1 || result.Stop.CallID != "save" || result.Cleanup != superagent.CleanupReleased ||
		f.env.scanned.Load() != 13 || f.env.revokes.Load() != 2 || f.env.releases.Load() != 1 {
		t.Fatalf("effect %q (%v), result %+v, stop %+v, executions %d", data, err, result, result.Stop, f.hits.Load())
	}
	events := allRunEvents(t, observeRun(t, ctx, run))
	last := len(events) - 1
	if events[last].Outcome != superagent.ToolOutcomeUnknown || events[last-1].Tool.Status != superagent.ToolStatusUnknown ||
		events[last-1].Tool.CallID != "save" {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestSlowToolAndObservationIsolation(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		name := "closed"
		if overflow {
			name = "overflow"
		}
		t.Run(name, func(t *testing.T) {
			f := newSearchFixture(t)
			ctx := hostContext(t, "alice")
			entered, proceed := make(chan struct{}), make(chan struct{})
			b := f.cfg.LocalTools["search"]
			b.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
				close(entered)
				select {
				case <-proceed:
					return superagent.ToolResult{RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionKnown,
						Completion: &superagent.ToolCompletion{Outcome: superagent.ToolSucceeded}}, nil
				case <-ctx.Done():
					return superagent.ToolResult{RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionKnown,
						Completion: &superagent.ToolCompletion{Outcome: superagent.ToolCancelled},
						Reason:     &superagent.ToolReason{Code: "cancelled"}}, nil
				}
			})
			f.cfg.LocalTools["search"] = b
			beta := scripted(delta("beta"), completed("beta"))
			model := f.cfg.LocalModels["chat"]
			f.cfg.LocalModels["chat"] = modelFunc(func(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
				if req.RunID == "beta" {
					return beta, nil
				}
				return model.Generate(ctx, req)
			})
			rc := f.cfg.LocalRunners["main"]
			rc.ObserveBufferBytes = 800
			f.cfg.LocalRunners["main"] = rc
			client := newClient(t, f.cfg)
			alpha := start(t, ctx, client, f.request())
			a := observeRun(t, ctx, alpha)
			if overflow {
				f.first.events <- delta(strings.Repeat("a", 1024))
			}
			f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"))
			recv(t, ctx, entered)
			if overflow {
				if event, err := a.Recv(); err != nil || event.RunID != alpha.ID() || event.Seq != 1 {
					t.Fatalf("overflow prefix %+v, %v", event, err)
				}
				if _, err := a.Recv(); !errors.Is(err, superagent.ErrObserveOverflow) {
					t.Fatalf("observation overflow = %v", err)
				}
			} else if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			other := start(t, ctx, client, request("beta"))
			if r := wait(t, ctx, other, superagent.Succeeded); r.Answer.Text != "beta" || len(r.Messages) != 2 {
				t.Fatalf("other result %+v", r)
			}
			for _, event := range allRunEvents(t, observeRun(t, ctx, other)) {
				if event.RunID != "beta" || event.Tool != nil {
					t.Fatalf("cross-run event: %+v", event)
				}
			}
			if overflow {
				if err := alpha.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
				if r := wait(t, ctx, alpha, superagent.Cancelled); len(r.Messages) != 1 {
					t.Fatalf("cancelled messages: %+v", r)
				}
			} else {
				close(proceed)
				f.second.events <- completed("alpha")
				if r := wait(t, ctx, alpha, superagent.Succeeded); r.Answer.Text != "alpha" || len(r.Messages) != 4 {
					t.Fatalf("unobserved result: %+v", r)
				}
			}
		})
	}
}
