//go:build darwin || linux

package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
)

func (r *rig) interceptStart(input string, forward func(sdk.StartUpdate, chan<- sdk.StartUpdate) bool) {
	prior := r.app.services.start
	r.app.services.start = func(s *sdk.Session, ctx context.Context, req sdk.StartRequest) <-chan sdk.StartUpdate {
		source := prior(s, ctx, req)
		if req.Input.Text != input {
			return source
		}
		out := make(chan sdk.StartUpdate, 4)
		go func() {
			defer close(out)
			for update := range source {
				if !forward(update, out) {
					return
				}
			}
		}()
		return out
	}
}

func pause(ctx context.Context, gate <-chan struct{}) bool {
	select {
	case <-gate:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestPTYAdmissionNotifications(t *testing.T) {
	r := newRig(t, 1, 1, 3*time.Second)
	queued, queueGate, receiptGate := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r.interceptStart("notify-b", func(update sdk.StartUpdate, out chan<- sdk.StartUpdate) bool {
		switch update.Phase {
		case sdk.StartQueued:
			close(queued)
			if !pause(r.ctx, queueGate) {
				return false
			}
		case sdk.StartResolved:
			if !pause(r.ctx, receiptGate) {
				return false
			}
		}
		out <- update
		return true
	})
	r.launch()
	r.holdA()
	r.send("\tnotify-b\r")
	recv(t, r.ctx, queued)
	initial := r.expect("[乙 · 启动中]")
	if strings.Contains(initial, "排队中") {
		t.Fatalf("guessed queue state before admission notification: %q", initial)
	}
	close(queueGate)
	r.expect("[乙 · 排队中]")
	r.noDispatch()
	r.finishA()
	b := r.called("notify-b", 1)
	r.expect("[乙 · 受理中]")
	if r.audit.count("observe", b.request.RunID) != 0 {
		t.Fatal("observed before receiving the Run handle")
	}
	close(receiptGate)
	r.expect("[乙 · 运行中]")
	b.text("b-visible")
	r.expect("助手 · b-visible")
	b.complete("b-visible")
	r.expect("[乙 · 已完成]")
	r.saved(1, 1)
}

func TestPTYSwitchAndDraft(t *testing.T) {
	r := newRig(t, 2, 1, time.Second)
	r.launch()
	r.send("old-a\r")
	r.called("old-a", 1).complete("a-history")
	r.expect("[甲 · 已完成]", "助手 · a-history")
	r.send("草稿-A\t")
	r.expect("[乙 · 空闲]")
	r.send("   \r")
	r.expect("[乙 · 空闲]", ">    ")
	r.noDispatch()
	r.send("\x7f\x7f\x7fnew-b\r\r\t")
	b := r.called("new-b", 1)
	a := r.expect("[甲 · 已完成]", "> 草稿-A", "助手 · a-history")
	if strings.Contains(a, "new-b") {
		t.Fatalf("foreign input in A: %q", a)
	}
	b.complete("b-history")
	r.expect("[甲 · 已完成]", "乙 · 已完成")
	r.send("\t")
	screen := r.expect("[乙 · 已完成]", "助手 · b-history")
	if strings.Contains(screen, "a-history") || strings.Contains(screen, "草稿-A") || r.audit.count("start", b.request.RunID) != 1 {
		t.Fatalf("mixed session state or repeated admission: %q", screen)
	}
	r.noDispatch()
	r.saved(0, 1)
	r.saved(1, 1)
}

func TestPTYSingleSlotHandoff(t *testing.T) {
	t.Run("queue then start before A saves", func(t *testing.T) {
		r := newRig(t, 1, 1, 3*time.Second)
		saveGate := make(chan struct{})
		first := r.app.views[0].session
		commit := r.app.services.commit
		r.app.services.commit = func(s *sdk.Session, ctx context.Context, id sdk.RunID) (sdk.SessionCommit, error) {
			if s == first && !pause(ctx, saveGate) {
				return sdk.SessionCommit{}, ctx.Err()
			}
			return commit(s, ctx, id)
		}
		r.launch()
		r.holdA()
		r.send("\tqueued-b\r")
		r.expect("[乙 · 排队中]", "你 · queued-b")
		r.send("\t\t")
		r.expect("[甲 · 运行中]", "乙 · 排队中", "工具 · slow")
		r.expect("[乙 · 排队中]")
		r.noDispatch()
		if pending, err := r.app.views[1].session.PendingRun(r.ctx); err != nil || pending != "" {
			t.Fatalf("queue created a durable reservation: %q %v", pending, err)
		}
		r.finishA()
		b := r.called("queued-b", 1)
		r.expect("[乙 · 受理中]")
		r.expect("[乙 · 运行中]")
		b.text("b-stream")
		r.expect("甲 · 保存中", "助手 · b-stream")
		r.saved(0, 0)
		b.complete("b-stream-done")
		r.expect("[乙 · 已完成]")
		r.saved(1, 1)
		close(saveGate)
		r.expect("甲 · 已完成", "[乙 · 已完成]")
		r.saved(0, 1)
	})
	for _, tc := range []struct {
		name   string
		queued int
		status string
	}{{"full", 0, "队列已满"}, {"expired", 1, "等待超时"}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 1, tc.queued, 250*time.Millisecond)
			r.launch()
			r.holdA()
			r.send("\tdraft-b\r")
			if tc.queued > 0 {
				r.expect("[乙 · 排队中]")
			}
			r.expect("[乙 · "+tc.status+"]", "> draft-b", "甲 · 运行中")
			r.finishA()
			r.expect("甲 · 已完成")
			r.noDispatch()
			r.saved(1, 0)
		})
	}
}

func TestPTYCancelQueued(t *testing.T) {
	r := newRig(t, 1, 1, 3*time.Second)
	r.launch()
	a := r.holdA()
	r.send("\tqueued-cancel\r")
	r.expect("[乙 · 排队中]")
	r.send("\x1b\t")
	r.expect("[甲 · 运行中]", "乙 · 等待已取消", "工具 · slow")
	if r.audit.count("cancel", a.request.RunID) != 0 {
		t.Fatal("queue cancellation reached A")
	}
	r.finishA()
	r.expect("[甲 · 已完成]", "乙 · 等待已取消")
	r.send("\t")
	r.expect("[乙 · 等待已取消]", "> queued-cancel")
	r.noDispatch()
	r.saved(0, 1)
	r.saved(1, 0)
}
