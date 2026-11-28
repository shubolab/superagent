//go:build darwin || linux

package tui

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
)

func TestPTYTwoStreamsAndCancelTarget(t *testing.T) {
	r := newRig(t, 2, 2, 3*time.Second)
	cancelTarget, cancelGate := make(chan sdk.RunID, 2), make(chan struct{})
	cancel := r.app.services.cancel
	r.app.services.cancel = func(run *sdk.Run, ctx context.Context) error {
		cancelTarget <- run.ID()
		if !pause(ctx, cancelGate) {
			return ctx.Err()
		}
		return cancel(run, ctx)
	}
	r.launch()
	r.send("stream-a\r")
	a := r.called("stream-a", 1)
	a.text("alpha")
	r.expect("[甲 · 运行中]", "助手 · alpha")
	r.send("\tstream-b\r")
	b := r.called("stream-b", 1)
	b.text("beta")
	r.expect("[乙 · 运行中]", "助手 · beta")
	a.text("-background")
	r.send("\x1b[A\t")
	frame := r.expect("[甲 · 运行中]", "助手 · alpha-background", "乙 · 运行中")
	if strings.Contains(frame, "beta") {
		t.Fatalf("B output leaked into A: %q", frame)
	}
	r.send("\t")
	r.expect("[乙 · 运行中]", "助手 · beta")
	r.send("\x1b\t")
	r.expect("[甲 · 运行中]", "乙 · 取消中")
	if got := recv(t, r.ctx, cancelTarget); got != b.request.RunID {
		t.Fatalf("cancel targeted %q, want B %q", got, b.request.RunID)
	}
	close(cancelGate)
	r.expect("[甲 · 运行中]", "乙 · 已取消")
	a.text("-still-running")
	r.expect("[甲 · 运行中]", "助手 · alpha-background-still-running")
	a.complete("alpha-background-still-running")
	r.expect("[甲 · 已完成]", "乙 · 已取消")
	r.send("\x1b\t")
	afterB := r.expect("[乙 · 已取消]", "你 · stream-b")
	if strings.Contains(afterB, "助手 · beta") {
		t.Fatalf("unconfirmed cancelled preview entered history: %q", afterB)
	}
	for _, call := range []modelCall{a, b} {
		if r.audit.count("observe", call.request.RunID) != 1 || r.audit.count("commit", call.request.RunID) != 1 {
			t.Fatalf("observation / commit for %q = %d / %d", call.request.RunID,
				r.audit.count("observe", call.request.RunID), r.audit.count("commit", call.request.RunID))
		}
	}
	if r.audit.count("cancel", a.request.RunID) != 0 || r.audit.count("cancel", b.request.RunID) != 1 {
		t.Fatal("cancel leaked into the other session or ran more than once")
	}
}

func TestPTYCancelAdmissionRace(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "accepted"
		if lost {
			name = "lost acknowledgement"
		}
		t.Run(name, func(t *testing.T) {
			r := newRig(t, 1, 1, 3*time.Second)
			staleGate, receiptGate := make(chan struct{}), make(chan struct{})
			cancelTarget, cancelGate := make(chan sdk.RunID, 2), make(chan struct{})
			cancel := r.app.services.cancel
			r.app.services.cancel = func(run *sdk.Run, ctx context.Context) error {
				cancelTarget <- run.ID()
				if !pause(ctx, cancelGate) {
					return ctx.Err()
				}
				return cancel(run, ctx)
			}
			r.interceptStart("race-b", func(update sdk.StartUpdate, out chan<- sdk.StartUpdate) bool {
				if update.Phase != sdk.StartResolved {
					out <- update
					return true
				}
				if !pause(r.ctx, staleGate) {
					return false
				}
				out <- sdk.StartUpdate{RunID: update.RunID, Phase: sdk.StartQueued}
				if !pause(r.ctx, receiptGate) {
					return false
				}
				if lost {
					update.Run, update.Err = nil, sdk.ErrStartOutcomeUnknown
				}
				out <- update
				return true
			})
			r.launch()
			a := r.holdA()
			r.send("\trace-b\r")
			r.expect("[乙 · 排队中]")
			r.finishA()
			b := r.called("race-b", 1)
			r.expect("[乙 · 受理中]")
			r.send("\x1b")
			r.expect("[乙 · 取消中]")
			close(staleGate)
			afterStale := r.expect("[乙 · 取消中]")
			if strings.Contains(afterStale, "排队中") {
				t.Fatalf("late queue event reverted cancellation: %q", afterStale)
			}
			close(receiptGate)
			if lost {
				r.expect("[乙 · 受理待核对]")
			}
			if got := recv(t, r.ctx, cancelTarget); got != b.request.RunID {
				t.Fatalf("late admission cancelled %q, want %q", got, b.request.RunID)
			}
			r.send("\t")
			r.expect("[甲 · 已完成]")
			close(cancelGate)
			r.expect("[甲 · 已完成]", "乙 · 已取消")
			if r.audit.count("start", b.request.RunID) != 1 || r.audit.count("cancel", b.request.RunID) != 1 ||
				r.audit.count("cancel", a.request.RunID) != 0 {
				t.Fatal("late admission started or cancelled the wrong operation")
			}
			r.noDispatch()
			r.saved(0, 1)
			r.saved(1, 1)
		})
	}
	t.Run("terminal wins", func(t *testing.T) {
		r := newRig(t, 2, 1, 3*time.Second)
		finished, receiptGate := make(chan struct{}), make(chan struct{})
		wait, cancel := r.app.services.wait, r.app.services.cancel
		cancelled := make(chan sdk.RunID, 1)
		r.app.services.wait = func(run *sdk.Run, ctx context.Context) (sdk.Result, error) {
			result, err := wait(run, ctx)
			close(finished)
			if !pause(ctx, receiptGate) {
				return sdk.Result{}, ctx.Err()
			}
			return result, err
		}
		r.app.services.cancel = func(run *sdk.Run, ctx context.Context) error {
			err := cancel(run, ctx)
			cancelled <- run.ID()
			return err
		}
		r.launch()
		r.send("\tfinish-first\r")
		b := r.called("finish-first", 1)
		r.expect("[乙 · 运行中]")
		b.complete("already-finished")
		recv(t, r.ctx, finished)
		r.send("\x1b")
		r.expect("[乙 · 取消中]")
		if got := recv(t, r.ctx, cancelled); got != b.request.RunID {
			t.Fatal(got)
		}
		close(receiptGate)
		r.expect("[乙 · 已完成]", "助手 · already-finished")
		r.saved(1, 1)
	})
}

type interruptAfterPreview struct {
	sdk.EventStream
	preview bool
}

func (i *interruptAfterPreview) Recv() (sdk.Event, error) {
	if i.preview {
		return sdk.Event{}, sdk.ErrObserveOverflow
	}
	event, err := i.EventStream.Recv()
	i.preview = event.Kind == sdk.EventTextDelta
	return event, err
}

func TestPTYObservationAndCommit(t *testing.T) {
	r := newRig(t, 2, 1, 3*time.Second)
	observe, commit := r.app.services.observe, r.app.services.commit
	first := r.sessions[0]
	var firstObservation, failedSave atomic.Bool
	r.app.services.observe = func(run *sdk.Run, ctx context.Context) (sdk.EventStream, error) {
		stream, err := observe(run, ctx)
		if err == nil && firstObservation.CompareAndSwap(false, true) {
			return &interruptAfterPreview{EventStream: stream}, nil
		}
		return stream, err
	}
	r.app.services.commit = func(s *sdk.Session, ctx context.Context, id sdk.RunID) (sdk.SessionCommit, error) {
		if s == first && failedSave.CompareAndSwap(false, true) {
			r.audit.record("commit", id)
			return sdk.SessionCommit{}, sdk.ErrSessionStorage
		}
		return commit(s, ctx, id)
	}
	r.launch()
	r.send("observe-a\r")
	a := r.called("observe-a", 1)
	a.text("a-preview")
	r.expect("[甲 · 运行中]", "助手 · a-preview", "观察中断，等待结果")
	r.send("\tobserve-b\r")
	b := r.called("observe-b", 1)
	b.text("b-before")
	r.expect("[乙 · 运行中]", "助手 · b-before")
	a.complete("a-confirmed")
	r.expect("甲 · 待保存", "助手 · b-before")
	r.saved(0, 0)
	r.send("\t")
	waiting := r.expect("[甲 · 待保存]", "助手 · a-confirmed", "Enter 重试保存")
	if strings.Contains(waiting, "a-preview") || strings.Contains(waiting, "观察中断") {
		t.Fatalf("old preview remained after final result: %q", waiting)
	}
	r.send("\r\r")
	confirmed := r.expect("[甲 · 已完成]", "助手 · a-confirmed")
	if strings.Count(confirmed, "a-confirmed") != 1 {
		t.Fatalf("duplicate final answer: %q", confirmed)
	}
	r.saved(0, 1)
	r.send("\t")
	b.text("-after")
	r.expect("[乙 · 运行中]", "助手 · b-before-after")
	b.complete("b-before-after")
	r.expect("[乙 · 已完成]")
	r.saved(1, 1)
	if r.audit.count("observe", a.request.RunID) != 1 || r.audit.count("observe", b.request.RunID) != 1 ||
		r.audit.count("commit", a.request.RunID) != 2 || r.audit.count("commit", b.request.RunID) != 1 {
		t.Fatal("incorrect observation or history retry counts")
	}
	r.noDispatch()
}
