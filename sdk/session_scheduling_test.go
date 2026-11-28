package sdk

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/schedule"
)

type scheduledStart struct {
	run *Run
	err error
}

func beginScheduled(ctx context.Context, session *Session, req contract.StartRequest) <-chan scheduledStart {
	done := make(chan scheduledStart, 1)
	go func() {
		run, err := session.Start(ctx, req)
		done <- scheduledStart{run, err}
	}()
	return done
}

func scheduleReceive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func grantedRun(t *testing.T, ctx context.Context, ch <-chan scheduledStart, id contract.RunID) *Run {
	t.Helper()
	result := scheduleReceive(t, ctx, ch)
	if result.err != nil || result.run == nil || result.run.ID() != id {
		t.Fatalf("scheduled start %s: %+v", id, result)
	}
	return result.run
}

func awaitOccupancy(t *testing.T, ctx context.Context, pool *schedule.Scheduler, active, queued int) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		a, q := pool.Counts()
		if a == active && q == queued {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("capacity = %d/%d, want %d/%d", a, q, active, queued)
		case <-tick.C:
		}
	}
}

func scheduledClient(t *testing.T, assembly *Mesh) *Client {
	t.Helper()
	client, err := NewClient(assembly, "main")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func scheduleAssembly(t *testing.T, root string, limits *contract.SessionSchedulingConfig, model contract.ModelProvider, tool contract.Tool, customize ...func(*contract.RunnerConfig)) (*Mesh, *schedule.Scheduler) {
	t.Helper()
	cfg := sessionTestMeshConfig(t, root, "v1", time.Minute, model, tool)
	runner := cfg.LocalRunners["main"]
	runner.Scheduling = limits
	for _, configure := range customize {
		configure(&runner)
	}
	cfg.LocalRunners["main"] = runner
	assembly, err := NewMesh(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolved, _, err := assembly.local.ResolveSessionRunner(t.Context(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.SessionScheduler() == nil {
		return assembly, nil
	}
	return assembly, resolved.SessionScheduler().(*schedule.Scheduler)
}

func assertUnadmitted(t *testing.T, ctx context.Context, client *Client, s *Session, id contract.RunID) {
	t.Helper()
	if pending, err := s.PendingRun(ctx); err != nil || pending != "" {
		t.Fatalf("unadmitted request left session reservation: %q, %v", pending, err)
	}
	if _, err := client.Run(id).Observe(ctx); !errors.Is(err, contract.ErrRunNotFound) {
		t.Fatalf("unadmitted request exposed a Run stream: %v", err)
	}
}

func TestSessionSchedulingAssembly(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	model := deliveryModel(func(_ context.Context, _ contract.ModelRequest) (contract.ModelStream, error) {
		return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "plain"}}, nil
	})
	invalid := []contract.SessionSchedulingConfig{
		{},
		{MaxActiveRuns: 1, MaxQueuedRuns: -1},
		{MaxActiveRuns: 1, MaxQueuedRuns: 1},
		{MaxActiveRuns: 1, MaxQueueWait: -time.Second},
	}
	for _, limits := range invalid {
		cfg := sessionTestMeshConfig(t, root, "v1", time.Minute, model, nil)
		runner := cfg.LocalRunners["main"]
		runner.Scheduling = &limits
		cfg.LocalRunners["main"] = runner
		if _, err := NewMesh(cfg); !errors.Is(err, contract.ErrInvalidConfiguration) {
			t.Fatalf("invalid schedule configuration %+v: %v", limits, err)
		}
	}
	ordinary, pool := scheduleAssembly(t, root, nil, model, nil)
	if pool != nil {
		t.Fatal("ordinary runner acquired a scheduler")
	}
	client := scheduledClient(t, ordinary)
	run, err := client.Start(ctx, contract.StartRequest{ID: "ordinary", Model: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	waitSessionRun(t, ctx, run, contract.Succeeded)
	managed, scheduler := scheduleAssembly(t, root, &contract.SessionSchedulingConfig{MaxActiveRuns: 1}, model, nil)
	for range 2 {
		c := scheduledClient(t, managed)
		runner, _, err := c.sessionRunner(ctx)
		if err != nil || runner.SessionScheduler() != scheduler || scheduler == nil {
			t.Fatalf("clients did not share their runner's scheduler: %v", err)
		}
	}
	if _, err := scheduledClient(t, managed).Start(ctx, contract.StartRequest{ID: "raw", Model: "chat"}); !errors.Is(err, contract.ErrScheduleAdmissionRequired) {
		t.Fatalf("direct start bypassed scheduled entry: %v", err)
	}
}

func TestSessionSchedulingSingleSlot(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseTool := sync.OnceFunc(func() { close(release) })
	defer releaseTool()
	var tools, aModels, bModels atomic.Int32
	bDeadline := make(chan time.Time, 1)
	model := deliveryModel(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
		switch req.Messages[0].User.Text {
		case "A":
			aModels.Add(1)
			if req.Step == 1 {
				return sessionToolReply("slow"), nil
			}
			return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "A done"}}, nil
		case "B":
			bModels.Add(1)
			deadline, _ := ctx.Deadline()
			bDeadline <- deadline
			return textSessionStream(ctx, "B", " finished", nil), nil
		}
		return nil, errors.New("unexpected model input")
	})
	tool := deliveryTool(func(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
		tools.Add(1)
		close(entered)
		select {
		case <-release:
			return knownSessionTool(req), nil
		case <-ctx.Done():
			return contract.ToolResult{}, ctx.Err()
		}
	})
	assembly, pool := scheduleAssembly(t, root, &contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 1, MaxQueueWait: time.Second}, model, tool)
	clientA, clientB := scheduledClient(t, assembly), scheduledClient(t, assembly)
	a := openTestSession(t, ctx, clientA, root, "A")
	b := openTestSession(t, ctx, clientB, root, "B")
	aReq := sessionRequest(a, "A")
	aReq.Tools = []contract.ToolRef{"tool"}
	aRun := startSessionRun(t, ctx, a, aReq)
	scheduleReceive(t, ctx, entered)
	bReq := sessionRequest(b, "B")
	bReq.Budget.Timeout = address(450 * time.Millisecond)
	bStart := beginScheduled(ctx, b, bReq)
	awaitOccupancy(t, ctx, pool, 1, 1)
	assertUnadmitted(t, ctx, clientB, b, bReq.ID)
	if files, err := filepath.Glob(filepath.Join(root, "runs", "*.run")); err != nil || len(files) != 1 || bModels.Load() != 0 {
		t.Fatalf("queued B left execution state: files %v, calls %d, %v", files, bModels.Load(), err)
	}
	releasedAt := time.Now()
	releaseTool()
	waitSessionRun(t, ctx, aRun, contract.Succeeded)
	bRun := grantedRun(t, ctx, bStart, bReq.ID)
	if deadline := scheduleReceive(t, ctx, bDeadline); deadline.Before(releasedAt.Add(*bReq.Budget.Timeout)) {
		t.Fatalf("B's execution budget began while queued: %v", deadline)
	}
	events := observeSessionRun(t, ctx, bRun)
	if first := recvSessionEvent(t, events, bRun.ID(), contract.EventTextDelta); first.TextDelta != "B" {
		t.Fatalf("B first delta: %+v", first)
	}
	commitSessionRun(t, ctx, b, bRun.ID(), 1)
	assertSessionVersion(t, ctx, a, 0, 0)
	if pending, err := a.PendingRun(ctx); err != nil || pending != aRun.ID() {
		t.Fatalf("A lost its history reservation: %q, %v", pending, err)
	}
	waitSessionRun(t, ctx, aRun, contract.Succeeded)
	commitSessionRun(t, ctx, a, aRun.ID(), 1)
	awaitOccupancy(t, ctx, pool, 0, 0)
	if aModels.Load() != 2 || bModels.Load() != 1 || tools.Load() != 1 {
		t.Fatalf("model calls A/B=%d/%d, tools=%d", aModels.Load(), bModels.Load(), tools.Load())
	}
}

func TestSessionSchedulingWaitCancelAndTimeout(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	requests := make(chan contract.RunID, 8)
	model := deliveryModel(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
		requests <- req.RunID
		if req.Messages[0].User.Text == "A" {
			return textSessionStream(ctx, "A", " complete", release), nil
		}
		return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "B complete"}}, nil
	})
	assembly, pool := scheduleAssembly(t, root, &contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 1, MaxQueueWait: 300 * time.Millisecond}, model, nil)
	client := scheduledClient(t, assembly)
	a, b := openTestSession(t, ctx, client, root, "A"), openTestSession(t, ctx, client, root, "B")
	aRun := startSessionRun(t, ctx, a, sessionRequest(a, "A"))
	if got := scheduleReceive(t, ctx, requests); got != aRun.ID() {
		t.Fatal(got)
	}
	for _, tc := range []struct {
		name string
		want error
	}{
		{"manual", context.Canceled},
		{"caller", context.DeadlineExceeded},
		{"host", contract.ErrScheduleQueueTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bCtx context.Context
			var cancel context.CancelFunc
			if tc.name == "caller" {
				bCtx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
			} else {
				bCtx, cancel = context.WithCancel(ctx)
			}
			defer cancel()
			req := sessionRequest(b, tc.name)
			start := beginScheduled(bCtx, b, req)
			awaitOccupancy(t, ctx, pool, 1, 1)
			if tc.name == "manual" {
				cancel()
			}
			result := scheduleReceive(t, ctx, start)
			if result.run != nil || !errors.Is(result.err, tc.want) {
				t.Fatalf("cancel or timeout returned %+v, want %v", result, tc.want)
			}
			awaitOccupancy(t, ctx, pool, 1, 0)
			assertUnadmitted(t, ctx, client, b, req.ID)
			assertSessionVersion(t, ctx, b, 0, 0)
		})
	}
	req := sessionRequest(b, "resubmitted")
	retry := beginScheduled(ctx, b, req)
	awaitOccupancy(t, ctx, pool, 1, 1)
	unblock()
	waitSessionRun(t, ctx, aRun, contract.Succeeded)
	run := grantedRun(t, ctx, retry, req.ID)
	waitSessionRun(t, ctx, run, contract.Succeeded)
	if got := scheduleReceive(t, ctx, requests); got != run.ID() || len(requests) != 0 {
		t.Fatalf("removed request dispatched: %s, remaining %d", got, len(requests))
	}
}

func TestSessionSchedulingFIFOAndDedup(t *testing.T) {
	for queue := range 3 {
		t.Run(string(rune('0'+queue)), func(t *testing.T) {
			root, ctx := t.TempDir(), sessionTestContext(t)
			finishA, finishB := make(chan struct{}), make(chan struct{})
			releaseA := sync.OnceFunc(func() { close(finishA) })
			releaseB := sync.OnceFunc(func() { close(finishB) })
			defer releaseA()
			defer releaseB()
			dispatched := make(chan contract.RunID, 12)
			model := deliveryModel(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
				dispatched <- req.RunID
				switch req.Messages[0].User.Text {
				case "A":
					return textSessionStream(ctx, "A", " done", finishA), nil
				case "B":
					return textSessionStream(ctx, "B", " done", finishB), nil
				default:
					return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done"}}, nil
				}
			})
			assembly, pool := scheduleAssembly(t, root, &contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: queue, MaxQueueWait: 3 * time.Second}, model, nil)
			client := scheduledClient(t, assembly)
			a := openTestSession(t, ctx, client, root, "A")
			b := openTestSession(t, ctx, client, root, "B")
			c := openTestSession(t, ctx, client, root, "C")
			aRun := startSessionRun(t, ctx, a, sessionRequest(a, "A"))
			if got := scheduleReceive(t, ctx, dispatched); got != aRun.ID() {
				t.Fatal(got)
			}
			bReq, cReq := sessionRequest(b, "B"), sessionRequest(c, "C")
			if queue == 0 {
				if _, err := b.Start(ctx, bReq); !errors.Is(err, contract.ErrScheduleQueueFull) {
					t.Fatalf("zero waiting capacity: %v", err)
				}
				assertUnadmitted(t, ctx, client, b, bReq.ID)
				releaseA()
				waitSessionRun(t, ctx, aRun, contract.Succeeded)
				return
			}
			bStart := beginScheduled(ctx, b, bReq)
			awaitOccupancy(t, ctx, pool, 1, 1)
			duplicate := beginScheduled(ctx, b, bReq)
			followerCtx, cancelFollower := context.WithCancel(ctx)
			follower := beginScheduled(followerCtx, b, bReq)
			changed := bReq
			changed.Input.Text = "changed"
			if _, err := b.Start(ctx, changed); !errors.Is(err, contract.ErrStartConflict) {
				t.Fatalf("same ID, changed input: %v", err)
			}
			if _, err := b.Start(ctx, sessionRequest(b, "other")); !errors.Is(err, ErrSessionBusy) {
				t.Fatalf("multiple requests queued in one session: %v", err)
			}
			cancelFollower()
			if err := scheduleReceive(t, ctx, follower).err; !errors.Is(err, context.Canceled) {
				t.Fatalf("duplicate canceled: %v", err)
			}
			awaitOccupancy(t, ctx, pool, 1, 1)
			var cStart <-chan scheduledStart
			if queue == 1 {
				if _, err := c.Start(ctx, cReq); !errors.Is(err, contract.ErrScheduleQueueFull) {
					t.Fatalf("queue full: %v", err)
				}
			} else {
				cStart = beginScheduled(ctx, c, cReq)
				awaitOccupancy(t, ctx, pool, 1, 2)
			}
			releaseA()
			waitSessionRun(t, ctx, aRun, contract.Succeeded)
			bRun := grantedRun(t, ctx, bStart, bReq.ID)
			grantedRun(t, ctx, duplicate, bReq.ID)
			if got := scheduleReceive(t, ctx, dispatched); got != bReq.ID {
				t.Fatalf("FIFO gave B's place to %s", got)
			}
			d := openTestSession(t, ctx, client, root, "D")
			dReq := sessionRequest(d, "D")
			dStart := beginScheduled(ctx, d, dReq)
			awaitOccupancy(t, ctx, pool, 1, queue)
			releaseB()
			waitSessionRun(t, ctx, bRun, contract.Succeeded)
			if cStart != nil {
				cRun := grantedRun(t, ctx, cStart, cReq.ID)
				waitSessionRun(t, ctx, cRun, contract.Succeeded)
				if got := scheduleReceive(t, ctx, dispatched); got != cReq.ID {
					t.Fatalf("late request overtook C: %s", got)
				}
			}
			dRun := grantedRun(t, ctx, dStart, dReq.ID)
			waitSessionRun(t, ctx, dRun, contract.Succeeded)
			if got := scheduleReceive(t, ctx, dispatched); got != dReq.ID || len(dispatched) != 0 {
				t.Fatalf("unexpected/repeated dispatch %s, remaining %d", got, len(dispatched))
			}
		})
	}
}
