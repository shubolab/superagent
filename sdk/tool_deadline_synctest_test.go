package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestDeadlineAndBoundedToolReceiptWithSyntheticTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		invoked := make(chan struct{})
		stopped := make(chan error, 1)
		late := make(chan struct{})
		defer close(late)
		var modelCalls int
		b := contract.Budget{
			Timeout: address(time.Second), MaxModelRequests: address(uint64(2)),
			MaxModelInputBytes: address(uint64(4096)), MaxOutputBytes: address(uint64(4096)),
			MaxToolRequests: address(uint64(2)),
		}
		m, err := NewMesh(MeshConfig{
			LocalRunners: map[contract.RunnerRef]contract.RunnerConfig{"main": {
				AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"chat"}},
				AllowedTools:  map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {"tool": {}}},
				DefaultBudget: b, BudgetCaps: b, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
			}},
			LocalModels: map[contract.ModelRef]contract.ModelProvider{"chat": deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
				modelCalls++
				return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishToolCalls, ToolCalls: []contract.ModelToolCall{{
					CallID: "c1", Tool: "tool", Arguments: json.RawMessage(`{}`),
				}, {CallID: "other", Tool: "tool", Arguments: json.RawMessage(`{}`)}}}}, nil
			})},
			LocalTools: map[contract.ToolRef]contract.LocalToolBinding{"tool": {
				Spec: contract.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: contract.ToolLimits{MaxResultBytes: 4096}},
				Tool: deliveryTool(func(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
					close(invoked)
					<-ctx.Done()
					stopped <- ctx.Err()
					<-late
					return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
						Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded}}, nil
				}),
			}},
			ResolveCaller: func(context.Context) (contract.SubjectID, error) { return "owner", nil },
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
		run, err := client.Start(ctx, contract.StartRequest{ID: "run", Model: "chat", Tools: []contract.ToolRef{"tool"}})
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
		if err != nil || result.Outcome != contract.ToolOutcomeUnknown || result.Stop.CallID != "c1" ||
			len(result.Messages) != 1 || modelCalls != 1 || time.Since(started) != 1200*time.Millisecond {
			t.Errorf("result at %v: %+v, %v, model calls %d", time.Since(started), result, err, modelCalls)
		}
	})
}
