//go:build goexperiment.synctest

package superagent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestDeadlineAndBoundedToolReceiptWithSyntheticTime(t *testing.T) {
	synctest.Run(func() {
		ctx := context.Background()
		invoked := make(chan struct{})
		stopped := make(chan error, 1)
		late := make(chan struct{})
		defer close(late)
		var modelCalls int
		b := Budget{
			Timeout: address(time.Second), MaxModelRequests: address(uint64(2)),
			MaxModelInputBytes: address(uint64(4096)), MaxOutputBytes: address(uint64(4096)),
			MaxToolRequests: address(uint64(2)),
		}
		m, err := NewMesh(MeshConfig{
			LocalRunners: map[RunnerRef]RunnerConfig{"main": {
				AllowedModels: map[SubjectID][]ModelRef{"owner": {"chat"}},
				AllowedTools:  map[SubjectID]map[ToolRef]ToolPermission{"owner": {"tool": {}}},
				DefaultBudget: b, BudgetCaps: b, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
			}},
			LocalModels: map[ModelRef]ModelProvider{"chat": deliveryModel(func(context.Context, ModelRequest) (ModelStream, error) {
				modelCalls++
				return &completionStream{&ModelReply{Finish: ModelFinishToolCalls, ToolCalls: []ModelToolCall{{
					CallID: "c1", Tool: "tool", Arguments: json.RawMessage(`{}`),
				}, {CallID: "other", Tool: "tool", Arguments: json.RawMessage(`{}`)}}}}, nil
			})},
			LocalTools: map[ToolRef]LocalToolBinding{"tool": {
				Spec: ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: ToolLimits{MaxResultBytes: 4096}},
				Tool: deliveryTool(func(ctx context.Context, req ToolRequest) (ToolResult, error) {
					close(invoked)
					<-ctx.Done()
					stopped <- ctx.Err()
					<-late
					return ToolResult{RunID: req.RunID, CallID: req.CallID, State: ToolExecutionKnown,
						Completion: &ToolCompletion{Outcome: ToolSucceeded}}, nil
				}),
			}},
			ResolveCaller: func(context.Context) (SubjectID, error) { return "owner", nil },
		})
		if err != nil {
			t.Error(err)
			return
		}
		client, err := NewClient(m, "main")
		if err != nil {
			t.Error(err)
			return
		}
		started := time.Now()
		run, err := client.Start(ctx, StartRequest{ID: "run", Model: "chat", Tools: []ToolRef{"tool"}})
		if err != nil {
			t.Error(err)
			return
		}
		<-invoked
		if got := time.Since(started); got != 0 {
			t.Errorf("dispatch advanced synthetic time: %v", got)
		}
		if err := <-stopped; !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != time.Second {
			t.Errorf("tool observed deadline at %v with %v", time.Since(started), err)
		}
		result, err := run.Wait(ctx)
		if err != nil || result.Outcome != ToolOutcomeUnknown || result.Stop.CallID != "c1" ||
			len(result.Messages) != 1 || modelCalls != 1 || time.Since(started) != 1200*time.Millisecond {
			t.Errorf("result at %v: %+v, %v, model calls %d", time.Since(started), result, err, modelCalls)
		}
	})
}
