package superagent_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/shubolab/superagent"
)

func TestStartRejectsBeforeModelDispatch(t *testing.T) {
	model := newProbe(nil)
	client := newClient(t, hostConfig(model))
	for _, tc := range []struct {
		name    string
		subject superagent.SubjectID
		change  func(*superagent.StartRequest)
		want    error
	}{
		{"no identity", "", func(*superagent.StartRequest) {}, superagent.ErrAccessDenied},
		{"model not allowed", "charlie", func(*superagent.StartRequest) {}, superagent.ErrAccessDenied},
		{"model not registered", "alice", func(r *superagent.StartRequest) { r.Model = "missing" }, superagent.ErrBindingNotFound},
		{"empty id", "alice", func(r *superagent.StartRequest) { r.ID = "" }, superagent.ErrInvalidRequest},
		{"bad history", "alice", func(r *superagent.StartRequest) { r.History = []superagent.Message{{}} }, superagent.ErrInvalidRequest},
		{"invalid budget", "alice", func(r *superagent.StartRequest) { r.Budget.MaxModelRequests = pointer(uint64(0)) }, superagent.ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := request("id")
			tc.change(&req)
			if run, err := client.Start(hostContext(t, tc.subject), req); run != nil || !errors.Is(err, tc.want) {
				t.Fatalf("Start = %v, %v, want %v", run, err, tc.want)
			}
		})
	}
	if n := model.count.Load(); n != 0 {
		t.Fatalf("invalid starts dispatched %d model requests", n)
	}
	if _, err := client.Run("id").Wait(hostContext(t, "alice")); !errors.Is(err, superagent.ErrRunNotFound) {
		t.Fatalf("rejected Run lookup = %v", err)
	}
}

func TestStartFreezesInputAndDeduplicates(t *testing.T) {
	ctx := hostContext(t, "alice")
	stream := scripted(delta("你"))
	model := newProbe(map[superagent.RunID]*controlledStream{"id": stream})
	cfg := hostConfig(model)
	client := newClient(t, cfg)
	delete(cfg.LocalModels, "chat")
	delete(cfg.LocalRunners["main"].AllowedModels, "alice")
	user := superagent.UserMessage{Text: "prior question"}
	req := request("id")
	req.Instruction = "answer briefly"
	req.History = []superagent.Message{{User: &user}, {Assistant: &superagent.AssistantMessage{Text: "prior answer"}}}
	req.Budget.MaxOutputBytes = pointer(uint64(4096))
	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	run := start(t, startCtx, client, req)
	cancel()
	user.Text = "changed after Start"
	call := recv(t, ctx, model.calls)
	if call.ctx.Err() != nil || call.req.RunID != "id" || call.req.Step != 1 || call.req.Model != "chat" ||
		call.req.Instruction != "answer briefly" || call.req.MaxOutputBytes != 1024 {
		t.Fatalf("model call = %+v, context error = %v", call.req, call.ctx.Err())
	}
	wantMessages := []superagent.Message{
		{User: &superagent.UserMessage{Text: "prior question"}},
		{Assistant: &superagent.AssistantMessage{Text: "prior answer"}},
		{User: &req.Input},
	}
	if !reflect.DeepEqual(call.req.Messages, wantMessages) {
		t.Fatalf("model request messages = %+v", call.req.Messages)
	}
	req.History[0].User = &superagent.UserMessage{Text: "prior question"}
	conflicting := req
	conflicting.Input.Text = "different"
	if _, err := client.Start(ctx, conflicting); !errors.Is(err, superagent.ErrStartConflict) {
		t.Fatalf("different request on same ID = %v", err)
	}
	retries := make(chan error, 4)
	for range cap(retries) {
		go func() {
			again, err := client.Start(ctx, req)
			if err == nil && again.ID() != run.ID() {
				err = errors.New("retry returned another run")
			}
			retries <- err
		}()
	}
	for range cap(retries) {
		if err := recv(t, ctx, retries); err != nil {
			t.Fatal(err)
		}
	}
	stream.events <- delta("好")
	stream.events <- completed("你好")
	result := wait(t, ctx, run, superagent.Succeeded)
	if result.Answer.Text != "你好" || len(result.Messages) != 2 ||
		result.Messages[0].User.Text != req.Input.Text || result.Messages[1].Assistant.Text != "你好" {
		t.Fatalf("message delta = %+v", result)
	}
	recv(t, ctx, stream.closed)
	start(t, ctx, client, req)
	if n := model.count.Load(); n != 1 {
		t.Fatalf("model requests = %d, want one", n)
	}
	if n := stream.closeHits.Load(); n != 1 {
		t.Fatalf("model close calls = %d, want one", n)
	}
}

func TestRunFailureBeforeValidCompletion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*controlledStream)
		code  superagent.StopCode
	}{
		{"early EOF", func(s *controlledStream) { close(s.events) }, superagent.StopModelTruncated},
		{"tool finish", func(s *controlledStream) {
			s.events <- superagent.ModelEvent{Kind: superagent.ModelCompleted, Reply: &superagent.ModelReply{Finish: superagent.ModelFinishToolCalls}}
		}, superagent.StopModelProtocol},
		{"missing reply", func(s *controlledStream) { s.events <- superagent.ModelEvent{Kind: superagent.ModelCompleted} }, superagent.StopModelProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			stream := scripted(delta("draft"))
			tc.setup(stream)
			model := newProbe(map[superagent.RunID]*controlledStream{"id": stream})
			run := start(t, ctx, newClient(t, hostConfig(model)), request("id"))
			result := wait(t, ctx, run, superagent.Failed)
			if result.Answer != nil || len(result.Messages) != 1 || result.Stop == nil || result.Stop.Code != tc.code {
				t.Fatalf("failure result = %+v / %+v", result, result.Stop)
			}
			recv(t, ctx, stream.closed)
			if n := model.count.Load(); n != 1 {
				t.Fatalf("model requests = %d, want one", n)
			}
			observe, err := run.Observe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if event, err := observe.Recv(); err != nil || event.Kind != superagent.EventRunStarted {
				t.Fatalf("first event = %+v, %v", event, err)
			}
			if event, err := observe.Recv(); err != nil || event.TextDelta != "draft" {
				t.Fatalf("draft event = %+v, %v", event, err)
			}
			if event, err := observe.Recv(); err != nil || event.Kind != superagent.EventRunFinished || event.Outcome != superagent.Failed {
				t.Fatalf("terminal event = %+v, %v", event, err)
			}
			_ = observe.Close()
		})
	}
}

func TestRunRejectsModelInputOverBudgetBeforeDispatch(t *testing.T) {
	ctx := hostContext(t, "alice")
	model := newProbe(nil)
	req := request("id")
	req.Budget.MaxModelInputBytes = pointer(uint64(1))
	r := start(t, ctx, newClient(t, hostConfig(model)), req)
	result := wait(t, ctx, r, superagent.BudgetExceeded)
	if model.count.Load() != 0 || result.Stop == nil || result.Stop.Code != superagent.StopModelInputLimit ||
		result.Stop.LimitBytes != 1 || len(result.Messages) != 1 {
		t.Fatalf("input limit result = %+v / %+v, model requests = %d", result, result.Stop, model.count.Load())
	}
}

func TestRunMeasuresOutputBytesWithoutRepeatingDraft(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  uint64
		events []superagent.ModelEvent
		want   superagent.RunOutcome
	}{
		{"six bytes", 6, []superagent.ModelEvent{delta("你"), delta("好"), completed("你好")}, superagent.Succeeded},
		{"five bytes", 5, []superagent.ModelEvent{delta("你"), delta("好"), completed("你好")}, superagent.BudgetExceeded},
		{"oversized snapshot", 5, []superagent.ModelEvent{delta("你"), completed("你好")}, superagent.BudgetExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			stream := scripted(tc.events...)
			model := newProbe(map[superagent.RunID]*controlledStream{"id": stream})
			req := request("id")
			req.Budget.MaxOutputBytes = &tc.limit
			run := start(t, ctx, newClient(t, hostConfig(model)), req)
			result := wait(t, ctx, run, tc.want)
			if tc.want == superagent.BudgetExceeded &&
				(result.Answer != nil || result.Stop == nil || result.Stop.Code != superagent.StopModelOutputLimit || result.Stop.LimitBytes != tc.limit) {
				t.Fatalf("output limit result = %+v / %+v", result, result.Stop)
			}
			recv(t, ctx, stream.closed)
		})
	}
}
