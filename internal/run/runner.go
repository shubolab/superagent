// Package run owns the lifecycle and results of admitted runs.
package run

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contextview"
	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
)

type OpenModel func(context.Context, contract.SubjectID, contract.ModelRequest) (contract.ModelStream, error)

type runKey struct {
	subject contract.SubjectID
	id      contract.RunID
}

type Runner struct {
	models               map[contract.ModelRef]struct{}
	allowed              map[contract.SubjectID]map[contract.ModelRef]struct{}
	defaults             limits
	caps                 limits
	observeBufferBytes   uint64
	retention            time.Duration
	open                 OpenModel
	ref                  contract.RunnerRef
	devices              mesh.MeshTransport
	tools                map[contract.ToolRef]contract.LocalToolBinding
	permissions          map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission
	defaultEnvironment   contract.EnvironmentRef
	contextPolicies      map[contract.ModelRef]contract.ModelContextPolicy
	recovery             *contract.RecoveryConfig
	retainsArtifacts     bool
	takeoverArtifacts    bool
	takeoverEnvironments map[contract.EnvironmentRef]contract.TakeoverWorkspace
	instance             string
	leaseClock           func() (time.Time, error)

	mu   sync.Mutex
	runs map[runKey]*state
}

func New(cfg contract.RunnerConfig, models map[contract.ModelRef]struct{}, open OpenModel) (*Runner, error) {
	defaults, defaultsOK := configuredBudget(cfg.DefaultBudget)
	caps, capsOK := configuredBudget(cfg.BudgetCaps)
	if !defaultsOK || !capsOK || defaults.Timeout > caps.Timeout || defaults.ModelRequests > caps.ModelRequests ||
		defaults.ModelInput > caps.ModelInput || defaults.Output > caps.Output || defaults.ToolRequests > caps.ToolRequests || defaults.Scan > caps.Scan || defaults.Raw > caps.Raw {
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
	for ref, policy := range cfg.ContextPolicies {
		if _, ok := models[ref]; !ok || policy.MaxOutputTokens == 0 ||
			(policy.Compaction != nil && !contextview.ValidPolicy(*policy.Compaction)) {
			return nil, fmt.Errorf("%w: invalid context policy for model %q", contract.ErrInvalidConfiguration, ref)
		}
	}

	policies := maps.Clone(cfg.ContextPolicies)
	for ref, policy := range policies {
		policy.Compaction = copyValue(policy.Compaction)
		policies[ref] = policy
	}
	recovery := copyValue(cfg.Recovery)
	if recovery != nil {
		recovery.Takeover = copyValue(recovery.Takeover)
		if recovery.Takeover != nil && recovery.Takeover.LeaseDuration <= 0 {
			return nil, fmt.Errorf("%w: lease duration must be positive", contract.ErrInvalidConfiguration)
		}
		if !durable.Supported || cfg.Recovery.BindingVersion == "" || cfg.Recovery.ReconcileTimeout <= 0 {
			return nil, fmt.Errorf("%w: invalid recovery configuration", contract.ErrInvalidConfiguration)
		}
		if err := durable.Directory(cfg.Recovery.Directory); err != nil {
			return nil, fmt.Errorf("%w: recovery directory: %v", contract.ErrInvalidConfiguration, err)
		}
	}
	return &Runner{
		recovery: recovery, instance: rand.Text(), leaseClock: func() (time.Time, error) { return time.Now(), nil },
		models: models, allowed: allowed, defaults: defaults, caps: caps, open: open, contextPolicies: policies,
		observeBufferBytes: cfg.ObserveBufferBytes, retention: cfg.ResultRetention, runs: make(map[runKey]*state),
	}, nil
}

func (r *Runner) Start(ctx context.Context, subject contract.SubjectID, req contract.StartRequest) (contract.RunID, error) {
	if req.ID == "" || (req.Takeover && !req.Recoverable) || !utf8.ValidString(req.Instruction) || !utf8.ValidString(req.Input.Text) || !validHistory(req.History) {
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
	req.Environment = copyValue(req.Environment)
	if req.Environment != nil && req.Environment.Ref == "" {
		req.Environment.Ref = r.defaultEnvironment
	}
	if err := r.admitTools(subject, req); err != nil {
		return "", err
	}
	policy := r.contextPolicies[req.Model].Compaction
	if len(req.PinnedHistory) > 0 && policy == nil {
		return "", contract.ErrInvalidRequest
	}
	for _, pin := range req.PinnedHistory {
		if pin < 0 || pin >= len(req.History) {
			return "", contract.ErrInvalidRequest
		}
	}
	if policy != nil && !slices.Contains(req.Tools, policy.SourceTool) {
		return "", contract.ErrAccessDenied
	}
	req.PinnedHistory = slices.Clone(req.PinnedHistory)
	req.History = cloneMessages(req.History)
	req.Tools = slices.Clone(req.Tools)
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
		if req.Recoverable && existing.request.Recoverable {
			return r.startRecoverable(ctx, subject, req, budget, now)
		}
		if !reflect.DeepEqual(existing.request, req) {
			return "", contract.ErrStartConflict
		}
		return req.ID, nil
	}
	if req.Recoverable {
		return r.startRecoverable(ctx, subject, req, budget, now)
	}
	if r.recovery != nil {
		path, identity := r.recoveryPath(subject, req.ID)
		if _, err := readCheckpoint(path, identity); !errors.Is(err, contract.ErrRunNotFound) {
			if err != nil {
				return "", err
			}
			return "", contract.ErrStartConflict
		}
	}
	s := newState(req, budget, now.Add(budget.Timeout), r.observeBufferBytes)
	if policy != nil {
		s.compaction = &compactionState{sources: contextview.NewStore(policy.SourcePageBytes)}
	}
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
	if s.checkpoint != nil && s.checkpoint.resumed {
		return nil, contract.ErrRecoveryObservation
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
		if s.checkpointErr != nil {
			return contract.Result{}, s.checkpointErr
		}
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
		s.stopLocked(contract.Cancelled, contract.StopCancelled)
	}
	return s.checkpointErr
}

func validHistory(history []contract.Message) bool {
	for i := 0; i < len(history); i++ {
		m := history[i]
		if m.Tool != nil {
			return false
		}
		switch {
		case m.User != nil && m.Assistant == nil && utf8.ValidString(m.User.Text):
		case m.Assistant != nil && m.User == nil && utf8.ValidString(m.Assistant.Text):
			if !validCalls(m.Assistant.ToolCalls, nil) {
				return false
			}
			for _, call := range m.Assistant.ToolCalls {
				i++
				if i == len(history) {
					return false
				}
				paired := history[i]
				if paired.Tool == nil || paired.User != nil || paired.Assistant != nil || paired.Tool.Tool != call.Tool ||
					paired.Tool.Result.CallID != call.CallID || !contract.ValidToolResult(paired.Tool.Result, paired.Tool.Result.RunID, call.CallID) ||
					paired.Tool.Result.State == contract.ToolExecutionUnknown {
					return false
				}
			}
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
		out[i] = contract.Message{User: copyValue(m.User), Assistant: cloneAssistant(m.Assistant), Tool: copyValue(m.Tool)}
		if out[i].Tool != nil {
			out[i].Tool.Result = cloneToolResult(out[i].Tool.Result)
		}
	}
	return out
}

func cloneBudget(b contract.Budget) contract.Budget {
	return contract.Budget{
		Timeout:               copyValue(b.Timeout),
		MaxModelRequests:      copyValue(b.MaxModelRequests),
		MaxModelInputBytes:    copyValue(b.MaxModelInputBytes),
		MaxOutputBytes:        copyValue(b.MaxOutputBytes),
		MaxToolRequests:       copyValue(b.MaxToolRequests),
		MaxToolScanBytes:      copyValue(b.MaxToolScanBytes),
		MaxToolRawOutputBytes: copyValue(b.MaxToolRawOutputBytes),
	}
}

func cloneResult(result contract.Result) contract.Result {
	result.Answer = cloneAssistant(result.Answer)
	result.Messages = cloneMessages(result.Messages)
	result.Usage = copyValue(result.Usage)
	result.Stop = copyValue(result.Stop)
	result.ContextCompactions = slices.Clone(result.ContextCompactions)
	return result
}

func copyValue[T any](p *T) *T {
	if p == nil {
		return nil
	}
	value := *p
	return &value
}

func cloneCalls(calls []contract.ModelToolCall) []contract.ModelToolCall {
	copy := slices.Clone(calls)
	for i := range copy {
		copy[i].Arguments = bytes.Clone(copy[i].Arguments)
	}
	return copy
}

func cloneAssistant(message *contract.AssistantMessage) *contract.AssistantMessage {
	clone := copyValue(message)
	if clone != nil {
		clone.ToolCalls = cloneCalls(clone.ToolCalls)
	}
	return clone
}

func cloneToolResult(result contract.ToolResult) contract.ToolResult {
	result.Completion = copyValue(result.Completion)
	if result.Completion != nil {
		result.Completion.Artifacts = slices.Clone(result.Completion.Artifacts)
	}
	result.Reason = copyValue(result.Reason)
	result.Usage = copyValue(result.Usage)
	if result.Usage != nil {
		result.Usage.ScannedBytes = copyValue(result.Usage.ScannedBytes)
		result.Usage.RawOutputBytes = copyValue(result.Usage.RawOutputBytes)
	}
	return result
}

func (r *Runner) ReadSource(ctx context.Context, subject contract.SubjectID, id contract.RunID, req mesh.ContextSourceRequest) (contract.ContextSourcePage, error) {
	s, err := r.get(subject, id)
	if err != nil || s.compaction == nil {
		return contract.ContextSourcePage{}, contract.ErrContextSourceUnavailable
	}
	return s.compaction.sources.ReadSource(ctx, req.Ref, req.Offset, req.MaxBytes)
}
