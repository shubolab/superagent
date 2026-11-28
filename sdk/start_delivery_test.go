package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

type dropStartReply struct {
	mesh.MeshTransport
	lost atomic.Bool
}

func (d *dropStartReply) Call(ctx context.Context, req mesh.MeshRequest) (mesh.MeshReply, error) {
	reply, err := d.MeshTransport.Call(ctx, req)
	if req.Operation == mesh.Start && err == nil && d.lost.CompareAndSwap(false, true) {
		return mesh.MeshReply{}, mesh.ErrResponseLost
	}
	return reply, err
}

type deliveryModel func(context.Context, contract.ModelRequest) (contract.ModelStream, error)

func (m deliveryModel) Generate(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
	return m(ctx, req)
}

type deliveryTool func(context.Context, contract.ToolRequest) (contract.ToolResult, error)

func (t deliveryTool) Invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	return t(ctx, req)
}

type completionStream struct{ reply *contract.ModelReply }

func (s *completionStream) Close() error { return nil }

func (s *completionStream) Recv() (contract.ModelEvent, error) {
	if s.reply == nil {
		return contract.ModelEvent{}, io.EOF
	}
	reply := s.reply
	s.reply = nil
	return contract.ModelEvent{Kind: contract.ModelCompleted, Reply: reply}, nil
}

func address[T any](v T) *T { return &v }

func TestLostStartResponseDoesNotRepeatRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var models, tools atomic.Int32
	budget := contract.Budget{
		Timeout: address(time.Minute), MaxModelRequests: address(uint64(2)),
		MaxModelInputBytes: address(uint64(8192)), MaxOutputBytes: address(uint64(4096)),
		MaxToolRequests: address(uint64(1)),
	}
	m, err := NewMesh(MeshConfig{
		LocalRunners: map[contract.RunnerRef]contract.RunnerConfig{"main": {
			AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"chat"}},
			AllowedTools:  map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {"tool": {}}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
		}},
		LocalModels: map[contract.ModelRef]contract.ModelProvider{"chat": deliveryModel(func(_ context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
			models.Add(1)
			reply := &contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done"}
			if req.Step == 1 {
				reply = &contract.ModelReply{Finish: contract.ModelFinishToolCalls, ToolCalls: []contract.ModelToolCall{{
					CallID: "once", Tool: "tool", Arguments: json.RawMessage(`{}`),
				}}}
			}
			return &completionStream{reply}, nil
		})},
		LocalTools: map[contract.ToolRef]contract.LocalToolBinding{"tool": {
			Spec: contract.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: contract.ToolLimits{MaxResultBytes: 4096}},
			Tool: deliveryTool(func(_ context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
				tools.Add(1)
				return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
					Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded}}, nil
			}),
		}},
		ResolveCaller: func(context.Context) (contract.SubjectID, error) { return "owner", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(m, "main")
	if err != nil {
		t.Fatal(err)
	}
	client.transport = &dropStartReply{MeshTransport: client.transport}
	req := contract.StartRequest{ID: "id", Model: "chat", Tools: []contract.ToolRef{"tool"}}
	if run, err := client.Start(ctx, req); run != nil || !errors.Is(err, contract.ErrStartOutcomeUnknown) {
		t.Fatalf("lost response returned %v, %v", run, err)
	}
	first, err := client.Run(req.ID).Wait(ctx)
	if err != nil || first.Outcome != contract.Succeeded || len(first.Messages) != 4 {
		t.Fatalf("query original result = %+v, %v", first, err)
	}
	retry, err := client.Start(ctx, req)
	if err != nil || retry.ID() != req.ID {
		t.Fatalf("retry = %v, %v", retry, err)
	}
	result, err := retry.Wait(ctx)
	if err != nil || result.Outcome != contract.Succeeded || models.Load() != 2 || tools.Load() != 1 {
		t.Fatalf("repeated activity: models %d, tools %d, result %+v, err %v", models.Load(), tools.Load(), result, err)
	}
}
