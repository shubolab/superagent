package tui

import (
	"context"
	"errors"
	"time"

	sdk "github.com/shubolab/superagent"
)

type historyReply struct {
	session *sdk.Session
	lines   []string
	err     error
}

type pendingReply struct {
	pending sdk.PendingRecovery
	cleared bool
	err     error
}

func (a *app) loadSessions() {
	for index, id := range a.cfg.Sessions {
		go a.loadSession(index, id)
	}
}

func (a *app) loadSession(index int, id sdk.SessionID) {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	session, err := a.services.open(a.cfg.Client, ctx, id, a.cfg.Store)
	target := target{index: index}
	if err != nil {
		a.post(target, historyReply{err: err})
		return
	}
	snapshot, err := session.Snapshot(ctx)
	if err != nil {
		a.post(target, historyReply{err: err})
		return
	}
	if !a.post(target, historyReply{session: session, lines: messageLines(snapshot.Messages)}) {
		return
	}
	p, err := session.InspectPending(ctx)
	reply := pendingReply{pending: p, err: err}
	if err == nil && p.State == sdk.RecoveryUnadmitted {
		resolved, err := session.DiscardUnadmitted(ctx, p.RunID)
		if err == nil {
			reply.pending = resolved
			reply.cleared = resolved.RunID == p.RunID && resolved.State == sdk.RecoveryUnadmitted
		}
		reply.err = err
	}
	a.post(target, reply)
}

func (a *app) showPending(index int, v *view, reply pendingReply) {
	p := reply.pending
	v.ready = true
	if reply.cleared || (reply.err == nil && p.State == sdk.RecoveryIdle) {
		v.status, v.draft = "空闲", []rune(p.Draft)
		return
	}
	v.op = &operation{id: p.RunID, recovered: true}
	target := target{index, p.RunID}
	switch {
	case reply.err != nil:
		v.status = "待核对"
	case p.State == sdk.RecoveryContinue:
		v.status = "可继续"
	case p.State == sdk.RecoverySave:
		v.op.saveReady = true
		a.save(target, v)
	default:
		v.status = "待核对"
	}
}

func (a *app) resume(v *view) {
	ctx, cancel := context.WithCancel(a.ctx)
	op := v.op
	op.startCancel, op.cancelRequested, op.cancelSent = cancel, false, false
	v.status = "申请继续"
	target, session := target{a.selected, op.id}, v.session
	go func() {
		for update := range a.services.resume(session, ctx, target.id) {
			if !a.post(target, update) {
				return
			}
		}
	}()
}

func (a *app) resumeUpdate(target target, v *view, update sdk.StartUpdate) {
	op := v.op
	switch update.Phase {
	case sdk.StartQueued:
		if !op.cancelRequested && v.status == "申请继续" {
			v.status = "恢复排队中"
		}
	case sdk.StartAdmitting:
		if !op.cancelRequested {
			v.status = "恢复中"
		}
	case sdk.StartResolved:
		op.startCancel()
		op.startCancel = nil
		switch {
		case errors.Is(update.Err, sdk.ErrPendingResultReady):
			op.saveReady = true
			a.save(target, v)
		case errors.Is(update.Err, sdk.ErrRecoveryBusy):
			v.status = "原进程占用"
		case errors.Is(update.Err, context.Canceled), errors.Is(update.Err, context.DeadlineExceeded),
			errors.Is(update.Err, sdk.ErrScheduleQueueFull), errors.Is(update.Err, sdk.ErrScheduleQueueTimeout):
			v.status = "可继续"
		case update.Err != nil || update.Run == nil || update.Run.ID() != target.id:
			v.status = "待核对"
		default:
			op.run, v.status = update.Run, "恢复中"
			if op.cancelRequested {
				v.status = "取消中"
				a.cancelRun(target, op)
			}
			if !a.exiting {
				a.waitRun(target, op)
			}
		}
	}
}

func (a *app) shutdown() {
	a.exiting = true
	for _, v := range a.views {
		if v.op != nil && v.op.startCancel != nil {
			v.op.startCancel()
		}
	}
	ctx, cancel := context.WithTimeout(a.ctx, 500*time.Millisecond)
	defer cancel()
	for a.finishing() {
		select {
		case <-ctx.Done():
			return
		case update := <-a.inbox:
			a.apply(update)
		}
	}
}

func (a *app) finishing() bool {
	for _, v := range a.views {
		if v.op != nil && (v.op.startCancel != nil || v.op.saving) {
			return true
		}
	}
	return false
}
