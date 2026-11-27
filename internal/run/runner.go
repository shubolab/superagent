// Package run owns the lifecycle and results of admitted runs.
package run

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
)

type OpenModel func(context.Context, contract.SubjectID, contract.ModelRequest) (contract.ModelStream, error)

type runKey struct {
	subject contract.SubjectID
	id      contract.RunID
}

type Runner struct {
	models             map[contract.ModelRef]struct{}
	allowed            map[contract.SubjectID]map[contract.ModelRef]struct{}
	defaults           limits
	caps               limits
	observeBufferBytes uint64
	retention          time.Duration
	open               OpenModel

	mu   sync.Mutex
	runs map[runKey]*state
}

func New(cfg contract.RunnerConfig, models map[contract.ModelRef]struct{}, open OpenModel) (*Runner, error) {
	defaults, defaultsOK := configuredBudget(cfg.DefaultBudget)
	caps, capsOK := configuredBudget(cfg.BudgetCaps)
	if !defaultsOK || !capsOK || defaults.timeout > caps.timeout || defaults.modelRequests > caps.modelRequests ||
		defaults.modelInput > caps.modelInput || defaults.output > caps.output {
		return nil, fmt.Errorf("%w: invalid default budget or caps", contract.ErrInvalidConfiguration)
	}
	if cfg.ObserveBufferBytes == 0 || cfg.ResultRetention <= 0 || open == nil {
		return nil, fmt.Errorf("%w: an observation buffer, retention and model opener are required", contract.ErrInvalidConfiguration)
	}
	allowed := make(map[contract.SubjectID]map[contract.ModelRef]struct{}, len(cfg.AllowedModels))
	for subject, refs := range cfg.AllowedModels {
		if subject == "" {
			return nil, fmt.Errorf("%w: empty allowed subject", contract.ErrInvalidConfiguration)
		}
		allowed[subject] = make(map[contract.ModelRef]struct{}, len(refs))
		for _, ref := range refs {
			if _, ok := models[ref]; !ok || ref == "" {
				return nil, fmt.Errorf("%w: allowed model %q is not registered", contract.ErrInvalidConfiguration, ref)
			}
			allowed[subject][ref] = struct{}{}
		}
	}
	return &Runner{
		models: models, allowed: allowed, defaults: defaults, caps: caps, open: open,
		observeBufferBytes: cfg.ObserveBufferBytes, retention: cfg.ResultRetention, runs: make(map[runKey]*state),
	}, nil
}

func (r *Runner) Start(ctx context.Context, subject contract.SubjectID, req contract.StartRequest) (contract.RunID, error) {
	if req.ID == "" || !utf8.ValidString(req.Instruction) || !utf8.ValidString(req.Input.Text) || !validHistory(req.History) {
		return "", contract.ErrInvalidRequest
	}
	if _, ok := r.models[req.Model]; !ok {
		return "", contract.ErrBindingNotFound
	}
	if _, ok := r.allowed[subject][req.Model]; !ok {
		return "", contract.ErrAccessDenied
	}
	budget, ok := requestBudget(req.Budget, r.defaults, r.caps)
	if !ok {
		return "", contract.ErrInvalidRequest
	}
	req.History = cloneMessages(req.History)
	req.Budget = cloneBudget(req.Budget)
	key := runKey{subject: subject, id: req.ID}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for k, s := range r.runs {
		if s.expired(now, r.retention) {
			delete(r.runs, k)
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if existing, ok := r.runs[key]; ok {
		if !reflect.DeepEqual(existing.request, req) {
			return "", contract.ErrStartConflict
		}
		return req.ID, nil
	}
	s := newState(req, budget, now.Add(budget.timeout), r.observeBufferBytes)
	r.runs[key] = s
	go r.generate(s, subject)
	return req.ID, nil
}

func (r *Runner) get(subject contract.SubjectID, id contract.RunID) (*state, error) {
	key := runKey{subject: subject, id: id}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.runs[key]
	if !ok {
		return nil, contract.ErrRunNotFound
	}
	if s.expired(time.Now(), r.retention) {
		delete(r.runs, key)
		return nil, contract.ErrRunNotFound
	}
	return s, nil
}

func (r *Runner) Observe(ctx context.Context, subject contract.SubjectID, id contract.RunID) (contract.EventStream, error) {
	s, err := r.get(subject, id)
	if err != nil {
		return nil, err
	}
	return s.observe(ctx)
}

func (r *Runner) Wait(ctx context.Context, subject contract.SubjectID, id contract.RunID) (contract.Result, error) {
	s, err := r.get(subject, id)
	if err != nil {
		return contract.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return contract.Result{}, err
	}
	select {
	case <-ctx.Done():
		return contract.Result{}, ctx.Err()
	case <-s.done:
		return cloneResult(s.result), nil
	}
}

func (r *Runner) Cancel(ctx context.Context, subject contract.SubjectID, id contract.RunID) error {
	s, err := r.get(subject, id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.runningLocked() {
		s.finishLocked(contract.Cancelled, nil, nil, &contract.StopDetail{Code: contract.StopCancelled})
	}
	return nil
}

func validHistory(history []contract.Message) bool {
	for _, m := range history {
		switch {
		case m.User != nil && m.Assistant == nil && utf8.ValidString(m.User.Text):
		case m.Assistant != nil && m.User == nil && utf8.ValidString(m.Assistant.Text):
		default:
			return false
		}
	}
	return true
}

func cloneMessages(in []contract.Message) []contract.Message {
	if len(in) == 0 {
		return nil
	}
	out := make([]contract.Message, len(in))
	for i, m := range in {
		out[i] = contract.Message{User: copyValue(m.User), Assistant: copyValue(m.Assistant)}
	}
	return out
}

func cloneBudget(b contract.Budget) contract.Budget {
	return contract.Budget{
		Timeout:            copyValue(b.Timeout),
		MaxModelRequests:   copyValue(b.MaxModelRequests),
		MaxModelInputBytes: copyValue(b.MaxModelInputBytes),
		MaxOutputBytes:     copyValue(b.MaxOutputBytes),
	}
}

func cloneResult(result contract.Result) contract.Result {
	result.Answer = copyValue(result.Answer)
	result.Messages = cloneMessages(result.Messages)
	result.Usage = copyValue(result.Usage)
	result.Stop = copyValue(result.Stop)
	return result
}

func copyValue[T any](p *T) *T {
	if p == nil {
		return nil
	}
	value := *p
	return &value
}
