// Package tui presents two SDK sessions in a local terminal.
package tui

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
	"unicode"

	sdk "github.com/shubolab/superagent"
)

type Config struct {
	Client   *sdk.Client
	Sessions [2]sdk.SessionID
	Store    sdk.SessionConfig
	Request  sdk.StartRequest
}

type services struct {
	open    func(*sdk.Client, context.Context, sdk.SessionID, sdk.SessionConfig) (*sdk.Session, error)
	start   func(*sdk.Session, context.Context, sdk.StartRequest) <-chan sdk.StartUpdate
	resume  func(*sdk.Session, context.Context, sdk.RunID) <-chan sdk.StartUpdate
	observe func(*sdk.Run, context.Context) (sdk.EventStream, error)
	wait    func(*sdk.Run, context.Context) (sdk.Result, error)
	cancel  func(*sdk.Run, context.Context) error
	commit  func(*sdk.Session, context.Context, sdk.RunID) (sdk.SessionCommit, error)
}

type app struct {
	cfg      Config
	services services
	ctx      context.Context
	inbox    chan notification
	views    [2]view
	selected int
	exiting  bool
	unsent   [2]bool
}

type view struct {
	session *sdk.Session
	draft   []rune
	history []string
	status  string
	ready   bool
	op      *operation
}

type operation struct {
	id              sdk.RunID
	input           string
	startCancel     context.CancelFunc
	observeCancel   context.CancelFunc
	run             *sdk.Run
	cancelRequested bool
	cancelSent      bool
	unknown         bool
	waiting         bool
	saving          bool
	saveReady       bool
	recovered       bool
	terminal        *sdk.Result
	finalLines      []string
	seq             uint64
	preview         []preview
	previewBytes    int
	truncated       bool
	interrupted     bool
	tool            string
}

type target struct {
	index int
	id    sdk.RunID
}

type notification struct {
	target
	value any
}

type observationEnded struct{}
type waitReply struct {
	result sdk.Result
	err    error
}
type saveReply struct {
	receipt sdk.SessionCommit
	turn    *sdk.SessionTurn
	err     error
}
type cancelReply struct{ err error }

func open(ctx context.Context, cfg Config) (*app, error) {
	if cfg.Client == nil || cfg.Sessions[0] == "" || cfg.Sessions[1] == "" || cfg.Sessions[0] == cfg.Sessions[1] || cfg.Request.Model == "" {
		return nil, sdk.ErrInvalidConfiguration
	}
	a := &app{
		cfg: cfg, ctx: ctx, inbox: make(chan notification, 64),
		services: services{
			open: (*sdk.Client).OpenSession, start: (*sdk.Session).StartWithProgress,
			resume:  (*sdk.Session).ResumeAdmittedWithProgress,
			observe: (*sdk.Run).Observe, wait: (*sdk.Run).Wait, cancel: (*sdk.Run).Cancel,
			commit: (*sdk.Session).CommitPendingResult,
		},
	}
	for i := range a.views {
		a.views[i].status = "读取中"
	}
	return a, nil
}

func (a *app) key(key rune) {
	v := &a.views[a.selected]
	switch key {
	case '\t':
		a.selected = 1 - a.selected
	case '\r', '\n':
		a.submit(v)
	case '\x1b':
		a.stop(v)
	case '\b', '\x7f':
		if v.ready && len(v.draft) > 0 && v.op == nil {
			v.draft = v.draft[:len(v.draft)-1]
		}
	default:
		if v.ready && v.op == nil && len(v.draft) < 4096 && unicode.IsPrint(key) {
			v.draft = append(v.draft, key)
		}
	}
}

func (a *app) submit(v *view) {
	if !v.ready {
		return
	}
	if v.op != nil {
		switch {
		case v.op.saveReady && !v.op.saving && v.status == "待保存":
			a.save(target{a.selected, v.op.id}, v)
		case v.status == "可继续" || v.status == "原进程占用":
			a.resume(v)
		}
		return
	}
	input := string(v.draft)
	if strings.TrimSpace(input) == "" {
		return
	}
	ctx, cancel := context.WithCancel(a.ctx)
	id := v.session.NewRunID()
	v.op = &operation{id: id, input: input, startCancel: cancel}
	v.draft, v.status = nil, "启动中"
	target := target{a.selected, id}
	session := v.session
	req := a.cfg.Request
	req.ID, req.Input = id, sdk.UserMessage{Text: input}
	go func() {
		for update := range a.services.start(session, ctx, req) {
			if !a.post(target, update) {
				return
			}
		}
	}()
}

func (a *app) stop(v *view) {
	op := v.op
	if op == nil || op.saveReady || op.cancelRequested || (op.startCancel == nil && op.run == nil) {
		return
	}
	op.cancelRequested = true
	if !op.unknown {
		v.status = "取消中"
	}
	target := target{a.selected, op.id}
	if op.startCancel != nil {
		op.startCancel()
	}
	if op.run != nil {
		a.cancelRun(target, op)
		if op.unknown {
			a.waitRun(target, op)
		}
	}
}

func (a *app) post(target target, value any) bool {
	select {
	case a.inbox <- notification{target, value}:
		return true
	case <-a.ctx.Done():
		return false
	}
}

func (a *app) apply(n notification) {
	v := &a.views[n.index]
	switch value := n.value.(type) {
	case historyReply:
		if value.err != nil {
			v.status = "历史不可用"
		} else {
			v.session, v.history, v.status = value.session, value.lines, "核对中"
		}
		return
	case pendingReply:
		a.showPending(n.index, v, value)
		return
	}
	op := v.op
	if op == nil || op.id != n.id {
		return
	}
	switch value := n.value.(type) {
	case sdk.StartUpdate:
		if value.RunID == n.id && op.startCancel != nil {
			a.admission(n.target, v, value)
		}
	case sdk.Event:
		if value.RunID == n.id && op.terminal == nil && value.Seq > op.seq {
			op.seq = value.Seq
			op.observe(value)
		}
	case observationEnded:
		if op.terminal == nil {
			op.interrupted = true
		}
	case waitReply:
		a.finish(n.target, v, value)
	case saveReply:
		if !op.saving {
			return
		}
		if errors.Is(value.err, sdk.ErrSessionResultUnavailable) || errors.Is(value.err, sdk.ErrSessionContent) {
			op.saving, op.saveReady, v.status = false, false, "待核对"
			return
		}
		if value.err != nil || value.receipt.RunID != op.id {
			op.saving, v.status = false, "待保存"
			return
		}
		var outcome sdk.RunOutcome
		if value.turn != nil {
			v.history = historyTail(v.history, messageLines(value.turn.Messages))
			outcome = value.turn.Outcome
		} else {
			v.history = historyTail(v.history, op.finalLines)
			outcome = op.terminal.Outcome
		}
		v.status, v.op = outcomeLabel(outcome), nil
	case cancelReply:
		if op.terminal == nil {
			if value.err != nil {
				if !op.unknown {
					v.status = "取消请求失败"
				}
				op.cancelRequested, op.cancelSent = false, false
			} else if op.unknown {
				v.status = "取消中"
				a.waitRun(n.target, op)
			}
		}
	}
}

func (a *app) admission(target target, v *view, update sdk.StartUpdate) {
	op := v.op
	if op.recovered {
		a.resumeUpdate(target, v, update)
		return
	}
	if update.Phase != sdk.StartResolved {
		if !op.cancelRequested {
			switch update.Phase {
			case sdk.StartQueued:
				if v.status == "启动中" {
					v.status = "排队中"
				}
			case sdk.StartAdmitting:
				if v.status == "启动中" || v.status == "排队中" {
					v.status = "受理中"
				}
			}
		}
		return
	}
	op.startCancel()
	op.startCancel = nil
	if update.Err != nil {
		if errors.Is(update.Err, sdk.ErrStartOutcomeUnknown) || errors.Is(update.Err, sdk.ErrSessionResultUnavailable) {
			a.checkAdmission(target, v)
			return
		}
		v.draft, v.op = []rune(op.input), nil
		if a.exiting && errors.Is(update.Err, context.Canceled) {
			a.unsent[target.index] = true
		}
		switch {
		case errors.Is(update.Err, sdk.ErrScheduleQueueFull):
			v.status = "队列已满"
		case errors.Is(update.Err, sdk.ErrScheduleQueueTimeout), errors.Is(update.Err, context.DeadlineExceeded):
			v.status = "等待超时"
		case errors.Is(update.Err, context.Canceled):
			v.status = "等待已取消"
		default:
			v.status = "启动失败"
		}
		return
	}
	if update.Run == nil || update.Run.ID() != target.id {
		a.checkAdmission(target, v)
		return
	}
	op.run = update.Run
	v.status = "运行中"
	if op.cancelRequested {
		v.status = "取消中"
		a.cancelRun(target, op)
	}
	if a.exiting {
		return
	}
	a.observeRun(target, op)
	a.waitRun(target, op)
}

func (a *app) checkAdmission(target target, v *view) {
	op := v.op
	op.unknown, v.status = true, "受理待核对"
	op.run = a.cfg.Client.Run(op.id)
	if op.cancelRequested {
		a.cancelRun(target, op)
	}
	a.waitRun(target, op)
}

func (a *app) observeRun(target target, op *operation) {
	ctx, cancel := context.WithCancel(a.ctx)
	op.observeCancel = cancel
	run := op.run
	go func() {
		defer cancel()
		stream, err := a.services.observe(run, ctx)
		if err != nil {
			a.post(target, observationEnded{})
			return
		}
		defer stream.Close()
		for {
			event, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) && ctx.Err() == nil {
					a.post(target, observationEnded{})
				}
				return
			}
			if !a.post(target, event) {
				return
			}
		}
	}()
}

func (a *app) waitRun(target target, op *operation) {
	if op.waiting {
		return
	}
	op.waiting = true
	run := op.run
	go func() {
		result, err := a.services.wait(run, a.ctx)
		a.post(target, waitReply{result, err})
	}()
}

func (a *app) cancelRun(target target, op *operation) {
	if op.cancelSent {
		return
	}
	op.cancelSent = true
	run := op.run
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 3*time.Second)
		defer cancel()
		a.post(target, cancelReply{a.services.cancel(run, ctx)})
	}()
}

func (a *app) finish(target target, v *view, reply waitReply) {
	op := v.op
	op.waiting = false
	if op.terminal != nil {
		return
	}
	if reply.err != nil || reply.result.RunID != op.id {
		if !op.unknown {
			v.status = "结果待核对"
		}
		return
	}
	op.terminal = &reply.result
	op.saveReady = true
	op.finalLines = messageLines(reply.result.Messages)
	op.preview, op.interrupted, op.truncated, op.tool = nil, false, false, ""
	if op.observeCancel != nil {
		op.observeCancel()
	}
	a.save(target, v)
}

func (a *app) save(target target, v *view) {
	v.op.saving, v.status = true, "保存中"
	session := v.session
	readTurn := v.op.terminal == nil
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
		defer cancel()
		receipt, err := a.services.commit(session, ctx, target.id)
		reply := saveReply{receipt: receipt, err: err}
		if err == nil && readTurn {
			turn, err := session.ReadTurn(ctx, receipt.Version)
			reply.turn, reply.err = &turn, err
		}
		a.post(target, reply)
	}()
}

func outcomeLabel(outcome sdk.RunOutcome) string {
	switch outcome {
	case sdk.Succeeded:
		return "已完成"
	case sdk.Cancelled:
		return "已取消"
	case sdk.TimedOut:
		return "运行超时"
	case sdk.BudgetExceeded:
		return "预算已用尽"
	case sdk.ToolOutcomeUnknown:
		return "工具结果待核对"
	default:
		return "运行失败"
	}
}
