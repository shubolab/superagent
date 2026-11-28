package superagent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSessionSchedulingCancelHandoff(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		name := "granted but unadmitted"
		if admitted {
			name = "admitted"
		}
		t.Run(name, func(t *testing.T) {
			root, ctx := t.TempDir(), sessionTestContext(t)
			aGate, toolEntered, toolCanceled, bGate := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			finishA := sync.OnceFunc(func() { close(aGate) })
			finishB := sync.OnceFunc(func() { close(bGate) })
			defer finishA()
			defer finishB()
			modelCalls := make(chan RunID, 8)
			model := deliveryModel(func(ctx context.Context, req ModelRequest) (ModelStream, error) {
				modelCalls <- req.RunID
				switch req.Messages[0].User.Text {
				case "A":
					return textSessionStream(ctx, "A", " done", aGate), nil
				case "B":
					return sessionToolReply("slow"), nil
				default:
					return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "done"}}, nil
				}
			})
			tool := deliveryTool(func(ctx context.Context, req ToolRequest) (ToolResult, error) {
				close(toolEntered)
				<-ctx.Done()
				close(toolCanceled)
				<-bGate
				return ToolResult{RunID: req.RunID, CallID: req.CallID, State: ToolExecutionNotExecuted,
					Reason: &ToolReason{Code: "cancelled_before_effect"}}, nil
			})
			assembly, pool := scheduleAssembly(t, root, &SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 2, MaxQueueWait: 2 * time.Second}, model, tool)
			client := scheduledClient(t, assembly)
			a, b, c := openTestSession(t, ctx, client, root, "A"), openTestSession(t, ctx, client, root, "B"), openTestSession(t, ctx, client, root, "C")
			aRun := startSessionRun(t, ctx, a, sessionRequest(a, "A"))
			scheduleReceive(t, ctx, modelCalls)
			bReq, cReq := sessionRequest(b, "B"), sessionRequest(c, "C")
			bReq.Tools = []ToolRef{"tool"}
			bCtx, cancelB := context.WithCancel(ctx)
			defer cancelB()
			if !admitted {
				// Pause B after the scheduler grants the slot, before C1 can reserve it.
				lock, err := b.store.AdmissionLock(ctx, bReq.ID)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			}
			bStart := beginScheduled(bCtx, b, bReq)
			awaitOccupancy(t, ctx, pool, 1, 1)
			cStart := beginScheduled(ctx, c, cReq)
			awaitOccupancy(t, ctx, pool, 1, 2)
			finishA()
			waitSessionRun(t, ctx, aRun, Succeeded)
			awaitOccupancy(t, ctx, pool, 1, 1)
			if !admitted {
				cancelB()
				if result := scheduleReceive(t, ctx, bStart); result.run != nil || !errors.Is(result.err, context.Canceled) {
					t.Fatalf("unadmitted grant: %+v", result)
				}
				assertUnadmitted(t, ctx, client, b, bReq.ID)
			} else {
				bRun := grantedRun(t, ctx, bStart, bReq.ID)
				scheduleReceive(t, ctx, toolEntered)
				cancelB()
				select {
				case <-toolCanceled:
					t.Fatal("canceling the admission context stopped the admitted Run")
				default:
				}
				if err := bRun.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
				scheduleReceive(t, ctx, toolCanceled)
				if active, queued := pool.Counts(); active != 1 || queued != 1 {
					t.Fatalf("cancellation request released early: %d/%d", active, queued)
				}
				finishB()
				waitSessionRun(t, ctx, bRun, Cancelled)
				if first := scheduleReceive(t, ctx, modelCalls); first != bRun.ID() {
					t.Fatalf("B model dispatch = %s", first)
				}
			}
			cRun := grantedRun(t, ctx, cStart, cReq.ID)
			waitSessionRun(t, ctx, cRun, Succeeded)
			if invoked := scheduleReceive(t, ctx, modelCalls); invoked != cReq.ID || len(modelCalls) != 0 {
				t.Fatalf("unexpected dispatch after cancellation: %s, remaining %d", invoked, len(modelCalls))
			}
			awaitOccupancy(t, ctx, pool, 0, 0)
		})
	}
}

func TestSessionSchedulingReleaseFailures(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	failureGate, failureStarted := make(chan struct{}), make(chan struct{})
	fail := sync.OnceFunc(func() { close(failureGate) })
	defer fail()
	model := deliveryModel(func(ctx context.Context, req ModelRequest) (ModelStream, error) {
		if req.Messages[0].User.Text == "fail" {
			close(failureStarted)
			select {
			case <-failureGate:
				return nil, errors.New("controlled model failure")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "done"}}, nil
	})
	assembly, pool := scheduleAssembly(t, root, &SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 2, MaxQueueWait: time.Second}, model, nil)
	c := scheduledClient(t, assembly)
	a, b, next := openTestSession(t, ctx, c, root, "A"), openTestSession(t, ctx, c, root, "B"), openTestSession(t, ctx, c, root, "next")
	rejected := sessionRequest(a, "rejected")
	rejected.Model = "missing"
	lock, err := a.store.AdmissionLock(ctx, rejected.ID)
	if err != nil {
		t.Fatal(err)
	}
	locked := sync.OnceFunc(func() { _ = lock.Close() })
	defer locked()
	aStart := beginScheduled(ctx, a, rejected)
	awaitOccupancy(t, ctx, pool, 1, 0)
	failed, following := sessionRequest(b, "fail"), sessionRequest(next, "following")
	bStart := beginScheduled(ctx, b, failed)
	awaitOccupancy(t, ctx, pool, 1, 1)
	cStart := beginScheduled(ctx, next, following)
	awaitOccupancy(t, ctx, pool, 1, 2)
	locked()
	if result := scheduleReceive(t, ctx, aStart); !errors.Is(result.err, ErrBindingNotFound) {
		t.Fatalf("expected definite admission rejection: %+v", result)
	}
	assertUnadmitted(t, ctx, c, a, rejected.ID)
	bRun := grantedRun(t, ctx, bStart, failed.ID)
	scheduleReceive(t, ctx, failureStarted)
	events := observeSessionRun(t, ctx, bRun)
	if err := events.Close(); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	_, err = bRun.Wait(waitCtx)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected independent wait timeout: %v", err)
	}
	if active, queued := pool.Counts(); active != 1 || queued != 1 {
		t.Fatalf("observation or Wait released a Run: %d/%d", active, queued)
	}
	fail()
	waitSessionRun(t, ctx, bRun, Failed)
	follow := grantedRun(t, ctx, cStart, following.ID)
	waitSessionRun(t, ctx, follow, Succeeded)
	waitSessionRun(t, ctx, bRun, Failed)
	commitSessionRun(t, ctx, b, bRun.ID(), 1)
	awaitOccupancy(t, ctx, pool, 0, 0)
}
