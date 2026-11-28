package schedule

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func schedulerForTest(t *testing.T) *Scheduler {
	t.Helper()
	s, err := New(contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 2, MaxQueueWait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func hold(t *testing.T, s *Scheduler) *Permit {
	t.Helper()
	var p *Permit
	if err := s.Admit(t.Context(), Session{Subject: "owner", ID: "A"}, "A", "input A", func(ctx context.Context) error {
		p = s.Permit(ctx, "owner", "A")
		p.Attach()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Release)
	return p
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("scheduler did not advance")
		var zero T
		return zero
	}
}

func TestWaitDeadlineWithControlledClock(t *testing.T) {
	s := schedulerForTest(t)
	now := time.Now()
	var elapsed atomic.Int64
	s.now = func() time.Time { return now.Add(time.Duration(elapsed.Load())) }
	clock := make(chan time.Time)
	waiting := make(chan time.Duration, 1)
	s.timer = func(d time.Duration) (<-chan time.Time, func()) {
		waiting <- d
		return clock, func() {}
	}
	p := hold(t, s)
	b := Session{Subject: "owner", ID: "B"}
	result := make(chan error, 1)
	go func() {
		result <- s.Admit(t.Context(), b, "B", "input B", func(context.Context) error {
			return errors.New("expired request was dispatched")
		})
	}()
	if remaining := receive(t, waiting); remaining != time.Minute {
		t.Fatalf("initial wait = %v", remaining)
	}
	elapsed.Store(int64(time.Minute))
	clock <- s.now()
	if err := receive(t, result); !errors.Is(err, contract.ErrScheduleQueueTimeout) {
		t.Fatalf("host deadline = %v", err)
	}
	p.Release()
	if a, q := s.Counts(); a != 0 || q != 0 {
		t.Fatalf("expired entry retained: %d/%d", a, q)
	}

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(10*time.Millisecond))
	defer cancel()
	earlier, _ := ctx.Deadline()
	a := &attempt{ctx: ctx, expires: earlier.Add(time.Second)}
	if err := waitError(a, a.expires); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("earlier caller deadline masked by host deadline: %v", err)
	}
}

func TestUncertainAdmissionAndDuplicateRelease(t *testing.T) {
	s := schedulerForTest(t)
	var original *Permit
	a := Session{Subject: "owner", ID: "A"}
	err := s.Admit(t.Context(), a, "A", "input A", func(ctx context.Context) error {
		original = s.Permit(ctx, "owner", "A")
		if s.Permit(ctx, "other", "A") != nil || s.Permit(ctx, "owner", "B") != nil {
			return errors.New("foreign permission accepted")
		}
		original.MarkUncertain()
		return contract.ErrStartOutcomeUnknown
	})
	if !errors.Is(err, contract.ErrStartOutcomeUnknown) {
		t.Fatal(err)
	}

	b := Session{Subject: "owner", ID: "B"}
	next, _, err := s.submit(t.Context(), b, "B", "input B")
	if err != nil {
		t.Fatal(err)
	}
	if active, queued := s.Counts(); active != 1 || queued != 1 {
		t.Fatalf("premature release: %d active, %d queued", active, queued)
	}
	if err := s.Admit(t.Context(), a, "A", "input A", func(ctx context.Context) error {
		if s.Permit(ctx, "owner", "A") != original || original.ConfirmStopped() {
			return errors.New("recovery did not retain the original permit")
		}
		original.Attach()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	original.ConfirmStopped()
	select {
	case <-next.ready:
		t.Fatal("uncertain reconciliation released an attached Run")
	default:
	}
	original.Release()
	original.Release()
	receive(t, next.ready)
	if active, queued := s.Counts(); active != 1 || queued != 0 || next.permit == original {
		t.Fatalf("incorrect promotion: %d active, %d queued", active, queued)
	}
	next.permit.Release()
}

func TestGrantVersusCancelOrder(t *testing.T) {
	for _, grantFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel first", true: "grant first"}[grantFirst], func(t *testing.T) {
			s := schedulerForTest(t)
			occupied := hold(t, s)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			b := Session{Subject: "owner", ID: "B"}
			a, _, err := s.submit(ctx, b, "B", "input B")
			if err != nil {
				t.Fatal(err)
			}
			if grantFirst {
				occupied.Release()
				cancel()
			} else {
				cancel()
				occupied.Release()
			}
			receive(t, a.ready)
			if grantFirst {
				if a.permit == nil || !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatal("grant-first branch lost its slot before admission decided")
				}
				a.permit.Release()
			} else if a.permit != nil || !errors.Is(a.err, context.Canceled) {
				t.Fatalf("cancel-first branch was promoted: %+v", a)
			}
			if active, queued := s.Counts(); active != 0 || queued != 0 {
				t.Fatalf("stranded capacity: %d/%d", active, queued)
			}
		})
	}
}
