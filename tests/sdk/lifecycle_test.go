package superagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

func observe(t *testing.T, ctx context.Context, r *superagent.Run) superagent.EventStream {
	t.Helper()
	stream, err := r.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

func nextEvent(t *testing.T, stream superagent.EventStream) superagent.Event {
	t.Helper()
	event, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func readToEOF(t *testing.T, stream superagent.EventStream) []superagent.Event {
	t.Helper()
	var events []superagent.Event
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func TestFastRunObservationMatchesFinalResult(t *testing.T) {
	ctx := hostContext(t, "alice")
	modelStream := scripted(delta("你"), delta("好"), completed("你好"))
	model := newProbe(map[superagent.RunID]*controlledStream{"id": modelStream})
	run := start(t, ctx, newClient(t, hostConfig(model)), request("id"))
	result := wait(t, ctx, run, superagent.Succeeded)
	events := readToEOF(t, observe(t, ctx, run))
	if len(events) < 4 || events[0].Kind != superagent.EventRunStarted ||
		events[len(events)-2].Kind != superagent.EventMessageCompleted ||
		events[len(events)-1].Kind != superagent.EventRunFinished {
		t.Fatalf("event order = %+v", events)
	}
	for i, event := range events {
		if event.Seq != uint64(i+1) || event.RunID != run.ID() {
			t.Fatalf("event %d is misidentified: %+v", i, event)
		}
	}
	var preview strings.Builder
	for _, e := range events[1 : len(events)-2] {
		if e.Kind != superagent.EventTextDelta || e.Step != 1 {
			t.Fatalf("unexpected preview event: %+v", e)
		}
		preview.WriteString(e.TextDelta)
	}
	last := events[len(events)-1]
	complete := events[len(events)-2]
	if last.Outcome != result.Outcome || last.Step != 0 || events[0].Step != 0 ||
		complete.Step != 1 || complete.Message == nil || complete.Message.Text != result.Answer.Text ||
		preview.String() != result.Answer.Text {
		t.Fatalf("preview %q or completed events disagree with result: %+v", preview.String(), events)
	}
}

func TestObserveCloseAndContextOnlyAffectObservation(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{
		{"close", superagent.ErrStreamClosed},
		{"context", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			modelStream := scripted()
			model := newProbe(map[superagent.RunID]*controlledStream{"id": modelStream})
			r := start(t, ctx, newClient(t, hostConfig(model)), request("id"))
			call := recv(t, ctx, model.calls)
			recv(t, ctx, modelStream.receiving)
			obsCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			stream := observe(t, obsCtx, r)
			if event := nextEvent(t, stream); event.Kind != superagent.EventRunStarted {
				t.Fatalf("first event = %+v", event)
			}
			waiting := make(chan struct{})
			received := make(chan error, 1)
			go func() {
				close(waiting)
				_, err := stream.Recv()
				received <- err
			}()
			recv(t, ctx, waiting)
			if tc.name == "close" {
				for range 2 {
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				cancel()
			}
			if err := recv(t, ctx, received); !errors.Is(err, tc.want) {
				t.Fatalf("observation stopped with %v, want %v", err, tc.want)
			}
			if err := call.ctx.Err(); err != nil {
				t.Fatalf("observation stopped model: %v", err)
			}
			if _, err := r.Observe(ctx); !errors.Is(err, superagent.ErrObservationAlreadyUsed) {
				t.Fatalf("second observation error = %v", err)
			}
			modelStream.events <- completed("still running")
			wait(t, ctx, r, superagent.Succeeded)
		})
	}
}

func TestObserveOverflowDrainsPrefixAndDoesNotBlockRun(t *testing.T) {
	ctx := hostContext(t, "alice")
	modelStream := scripted(delta("你"), delta("好"), completed("你好"))
	cfg := hostConfig(newProbe(map[superagent.RunID]*controlledStream{"id": modelStream}))
	first, err := json.Marshal(superagent.Event{RunID: "id", Seq: 1, Kind: superagent.EventRunStarted})
	if err != nil {
		t.Fatal(err)
	}
	runner := cfg.LocalRunners["main"]
	runner.ObserveBufferBytes = uint64(len(first))
	cfg.LocalRunners["main"] = runner
	r := start(t, ctx, newClient(t, cfg), request("id"))
	stream := observe(t, ctx, r)
	wait(t, ctx, r, superagent.Succeeded)
	if event := nextEvent(t, stream); event.Kind != superagent.EventRunStarted || event.Seq != 1 {
		t.Fatalf("overflow prefix = %+v", event)
	}
	if _, err := stream.Recv(); !errors.Is(err, superagent.ErrObserveOverflow) {
		t.Fatalf("result after overflow prefix = %v", err)
	}
}

func TestWaitDeadlineAndIndependentSnapshots(t *testing.T) {
	ctx := hostContext(t, "alice")
	modelStream := scripted()
	model := newProbe(map[superagent.RunID]*controlledStream{"id": modelStream})
	client := newClient(t, hostConfig(model))
	r := start(t, ctx, client, request("id"))
	call := recv(t, ctx, model.calls)
	short, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()
	if _, err := r.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short wait error = %v", err)
	}
	if call.ctx.Err() != nil {
		t.Fatalf("short wait stopped model: %v", call.ctx.Err())
	}
	other := hostContext(t, "bob")
	if _, err := client.Run(r.ID()).Wait(other); !errors.Is(err, superagent.ErrRunNotFound) {
		t.Fatalf("cross-subject wait error = %v", err)
	}
	if _, err := client.Run(r.ID()).Observe(other); !errors.Is(err, superagent.ErrRunNotFound) {
		t.Fatalf("cross-subject observe error = %v", err)
	}
	type waited struct {
		result superagent.Result
		err    error
	}
	results := make(chan waited, 3)
	for range cap(results) {
		go func() {
			result, err := client.Run(r.ID()).Wait(ctx)
			results <- waited{result, err}
		}()
	}
	event := completed("answer")
	event.Reply.Usage = &superagent.Usage{InputTokens: 4}
	modelStream.events <- event
	initial := recv(t, ctx, results)
	if initial.err != nil || initial.result.Answer == nil || initial.result.Usage == nil {
		t.Fatalf("initial wait = %+v", initial)
	}
	for range 2 {
		got := recv(t, ctx, results)
		if got.err != nil || !reflect.DeepEqual(got.result, initial.result) {
			t.Fatalf("concurrent wait = %+v, want %+v", got, initial)
		}
	}
	initial.result.Answer.Text = "edited"
	initial.result.Messages[0].User.Text = "changed"
	initial.result.Messages[1].Assistant.Text = "changed answer"
	initial.result.Usage.InputTokens = 999
	saved := wait(t, ctx, r, superagent.Succeeded)
	if saved.Answer.Text != "answer" || saved.Messages[0].User.Text != "greet" ||
		saved.Messages[1].Assistant.Text != "answer" || saved.Usage.InputTokens != 4 {
		t.Fatalf("changed snapshot leaked to runner: %+v", saved)
	}
}

func TestCancelWinsOverLateCompletion(t *testing.T) {
	ctx := hostContext(t, "alice")
	modelStream := scripted()
	late := completed("late")
	modelStream.late = &late
	model := newProbe(map[superagent.RunID]*controlledStream{"id": modelStream})
	client := newClient(t, hostConfig(model))
	r := start(t, ctx, client, request("id"))
	call := recv(t, ctx, model.calls)
	recv(t, ctx, modelStream.receiving)
	other := hostContext(t, "bob")
	if err := client.Run(r.ID()).Cancel(other); !errors.Is(err, superagent.ErrRunNotFound) {
		t.Fatalf("cross-subject cancel error = %v", err)
	}
	if call.ctx.Err() != nil {
		t.Fatal("another subject cancelled the model")
	}
	for range 2 {
		if err := r.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
	}
	result := wait(t, ctx, r, superagent.Cancelled)
	recv(t, ctx, modelStream.closed)
	if !errors.Is(call.ctx.Err(), context.Canceled) || result.Answer != nil || len(result.Messages) != 1 ||
		result.Stop == nil || result.Stop.Code != superagent.StopCancelled || modelStream.closeHits.Load() != 1 {
		t.Fatalf("cancel result = %+v / %+v, model error = %v", result, result.Stop, call.ctx.Err())
	}
	events := readToEOF(t, observe(t, ctx, r))
	if len(events) != 2 || events[1].Kind != superagent.EventRunFinished || events[1].Outcome != superagent.Cancelled {
		t.Fatalf("late completion appeared in events: %+v", events)
	}
}

func TestCancelAfterSuccessKeepsResult(t *testing.T) {
	ctx := hostContext(t, "alice")
	model := newProbe(map[superagent.RunID]*controlledStream{"id": scripted(completed("done"))})
	r := start(t, ctx, newClient(t, hostConfig(model)), request("id"))
	result := wait(t, ctx, r, superagent.Succeeded)
	if err := r.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if after := wait(t, ctx, r, superagent.Succeeded); !reflect.DeepEqual(result, after) {
		t.Fatalf("result changed after cancellation: %+v", after)
	}
}

func TestRunDeadlineClosesInFlightModel(t *testing.T) {
	ctx := hostContext(t, "alice")
	stream := scripted()
	model := newProbe(map[superagent.RunID]*controlledStream{"id": stream})
	req := request("id")
	req.Budget.Timeout = pointer(250 * time.Millisecond)
	r := start(t, ctx, newClient(t, hostConfig(model)), req)
	call := recv(t, ctx, model.calls)
	recv(t, ctx, stream.receiving)
	result := wait(t, ctx, r, superagent.TimedOut)
	recv(t, ctx, stream.closed)
	if !errors.Is(call.ctx.Err(), context.DeadlineExceeded) || result.Stop == nil || result.Stop.Code != superagent.StopDeadline {
		t.Fatalf("deadline result = %+v / %+v, model error = %v", result, result.Stop, call.ctx.Err())
	}
}

func TestConcurrentRunsStayIsolated(t *testing.T) {
	ctx := hostContext(t, "alice")
	leftModel, rightModel := scripted(), scripted(delta("right"))
	model := newProbe(map[superagent.RunID]*controlledStream{"left": leftModel, "right": rightModel})
	client := newClient(t, hostConfig(model))
	left := start(t, ctx, client, request("left"))
	recv(t, ctx, leftModel.receiving)
	right := start(t, ctx, client, request("right"))
	rightEvents := observe(t, ctx, right)
	nextEvent(t, rightEvents)
	if event := nextEvent(t, rightEvents); event.RunID != "right" || event.TextDelta != "right" {
		t.Fatalf("right's live delta = %+v", event)
	}
	if err := left.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	rightModel.events <- completed("right done")
	wait(t, ctx, left, superagent.Cancelled)
	wait(t, ctx, right, superagent.Succeeded)
	for _, event := range readToEOF(t, rightEvents) {
		if event.RunID != "right" {
			t.Fatalf("right received another Run's event: %+v", event)
		}
	}
	recv(t, ctx, leftModel.closed)
	recv(t, ctx, rightModel.closed)
	if n := model.count.Load(); n != 2 {
		t.Fatalf("model requests = %d, want two", n)
	}
}
