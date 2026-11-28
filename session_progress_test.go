package superagent

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/mesh"
)

func readStartProgress(t *testing.T, ctx context.Context, stream <-chan StartUpdate, id RunID, want ...StartPhase) StartUpdate {
	t.Helper()
	var phases []StartPhase
	var last StartUpdate
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case update, ok := <-stream:
			if !ok {
				if !slices.Equal(phases, want) {
					t.Fatalf("admission phases = %v, want %v", phases, want)
				}
				return last
			}
			if update.RunID != id {
				t.Fatalf("wrong RunID in update: %+v", update)
			}
			phases = append(phases, update.Phase)
			last = update
		}
	}
}

type cancelAfterReservation struct {
	mesh.SessionRunner
	cancel context.CancelFunc
	calls  int
}

func (r *cancelAfterReservation) SessionBinding(ctx context.Context) (string, error) {
	r.calls++
	if r.calls == 2 {
		r.cancel()
	}
	return r.SessionRunner.SessionBinding(ctx)
}

func TestStartProgressCancellationClearsUnattemptedReservation(t *testing.T) {
	ctx, root := sessionTestContext(t), t.TempDir()
	var models atomic.Int32
	client := sessionTestClient(t, root, "v1", time.Minute, deliveryModel(func(context.Context, ModelRequest) (ModelStream, error) {
		models.Add(1)
		return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "done"}}, nil
	}), nil)
	s := openTestSession(t, ctx, client, root, "cancellation")
	runner, err := s.runner(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start, cancel := context.WithCancel(ctx)
	defer cancel()
	req := sessionRequest(s, "original")
	req.Recoverable = true
	wrapper := &cancelAfterReservation{SessionRunner: runner, cancel: cancel}
	if run, err := s.start(start, wrapper, req); run != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("reservation cancellation: %v, %v", run, err)
	}
	assertUnadmitted(t, ctx, client, s, req.ID)
	if models.Load() != 0 {
		t.Fatal("unattempted reservation dispatched")
	}
	follow := sessionRequest(s, "retry")
	waitSessionRun(t, ctx, startSessionRun(t, ctx, s, follow), Succeeded)
}

func TestStartProgressAuthority(t *testing.T) {
	ctx, root := sessionTestContext(t), t.TempDir()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var requests atomic.Int32
	model := deliveryModel(func(ctx context.Context, req ModelRequest) (ModelStream, error) {
		requests.Add(1)
		if req.Messages[0].User.Text == "A" {
			return textSessionStream(ctx, "A", " done", release), nil
		}
		return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "done"}}, nil
	})
	assembly, pool := scheduleAssembly(t, root, &SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 1, MaxQueueWait: 250 * time.Millisecond}, model, nil)
	c := scheduledClient(t, assembly)
	a := openTestSession(t, ctx, c, root, "a")
	b := openTestSession(t, ctx, c, root, "b")
	other := openTestSession(t, ctx, c, root, "other")
	aReq := sessionRequest(a, "A")
	first := readStartProgress(t, ctx, a.StartWithProgress(ctx, aReq), aReq.ID, StartAdmitting, StartResolved)
	if first.Err != nil || first.Run == nil {
		t.Fatalf("first admission: %+v", first)
	}
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	bReq := sessionRequest(b, "cancelled B")
	bProgress := b.StartWithProgress(cancelled, bReq)
	awaitOccupancy(t, ctx, pool, 1, 1)
	otherReq := sessionRequest(other, "full")
	full := readStartProgress(t, ctx, other.StartWithProgress(ctx, otherReq), otherReq.ID, StartResolved)
	if !errors.Is(full.Err, ErrScheduleQueueFull) {
		t.Fatalf("full reply = %+v", full)
	}
	cancel()
	bResult := readStartProgress(t, ctx, bProgress, bReq.ID, StartQueued, StartResolved)
	if !errors.Is(bResult.Err, context.Canceled) {
		t.Fatalf("cancelled reply = %+v", bResult)
	}

	bReq = sessionRequest(b, "expired B")
	bResult = readStartProgress(t, ctx, b.StartWithProgress(ctx, bReq), bReq.ID, StartQueued, StartResolved)
	if !errors.Is(bResult.Err, ErrScheduleQueueTimeout) {
		t.Fatalf("expired reply = %+v", bResult)
	}

	bReq = sessionRequest(b, "admitted B")
	count := uint64(1)
	bReq.Budget.MaxModelRequests = &count
	bProgress = b.StartWithProgress(ctx, bReq)
	awaitOccupancy(t, ctx, pool, 1, 1)
	duplicate := b.StartWithProgress(ctx, bReq)
	count = 0
	assertUnadmitted(t, ctx, c, b, bReq.ID)
	unblock()
	waitSessionRun(t, ctx, first.Run, Succeeded)
	awaitOccupancy(t, ctx, pool, 0, 0)
	bResult = readStartProgress(t, ctx, bProgress, bReq.ID, StartQueued, StartAdmitting, StartResolved)
	same := readStartProgress(t, ctx, duplicate, bReq.ID, StartResolved)
	if bResult.Err != nil || same.Err != nil || bResult.Run == nil || same.Run == nil || requests.Load() != 2 {
		t.Fatalf("delayed and duplicate start: %+v, %+v, dispatches=%d", bResult, same, requests.Load())
	}
	waitSessionRun(t, ctx, bResult.Run, Succeeded)

	plain, _ := scheduleAssembly(t, root, nil, model, nil)
	s := openTestSession(t, ctx, scheduledClient(t, plain), root, "plain")
	req := sessionRequest(s, "plain")
	result := readStartProgress(t, ctx, s.StartWithProgress(ctx, req), req.ID, StartResolved)
	if result.Err != nil || result.Run == nil {
		t.Fatalf("unmanaged admission: %+v", result)
	}
	waitSessionRun(t, ctx, result.Run, Succeeded)
}
