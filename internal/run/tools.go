package run

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/internal/mesh"
)

const (
	receiptGrace = 200 * time.Millisecond
	cleanupGrace = 200 * time.Millisecond
)

func (r *Runner) ConfigureTools(ref contract.RunnerRef, transport mesh.MeshTransport, cfg contract.RunnerConfig,
	tools map[contract.ToolRef]contract.LocalToolBinding, environments map[contract.EnvironmentRef]contract.EnvironmentProvider, artifactStore bool) error {
	r.ref, r.devices = ref, transport
	if cfg.DefaultEnvironment != "" {
		if _, ok := environments[cfg.DefaultEnvironment]; !ok {
			return fmt.Errorf("%w: unknown default environment %q", contract.ErrInvalidConfiguration, cfg.DefaultEnvironment)
		}
		r.defaultEnvironment = cfg.DefaultEnvironment
	}
	r.tools = make(map[contract.ToolRef]contract.LocalToolBinding, len(tools))
	for ref, binding := range tools {
		binding.Spec.InputSchema = bytes.Clone(binding.Spec.InputSchema)
		r.tools[ref] = binding
	}
	r.permissions = make(map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission, len(cfg.AllowedTools))
	for subject, access := range cfg.AllowedTools {
		for tool, permission := range access {
			binding, ok := tools[tool]
			if subject == "" || !ok {
				return fmt.Errorf("%w: invalid tool permission for %q", contract.ErrInvalidConfiguration, tool)
			}
			if binding.Spec.NeedsArtifactOutput != permission.AllowArtifacts || (permission.AllowArtifacts && !artifactStore) {
				return fmt.Errorf("%w: invalid artifact grant for %q", contract.ErrInvalidConfiguration, tool)
			}
			if binding.Spec.NeedsReadAccess {
				env, registered := environments[permission.Environment.Ref]
				if !registered || permission.Environment.Workspace == "" ||
					!validReadPrefix(permission.ReadPrefix) {
					return fmt.Errorf("%w: invalid workspace permission for %q", contract.ErrInvalidConfiguration, tool)
				}
				if declares, ok := env.(interface {
					AllowsPrefix(contract.WorkspaceRef, string) bool
				}); ok &&
					!declares.AllowsPrefix(permission.Environment.Workspace, permission.ReadPrefix) {
					return fmt.Errorf("%w: workspace does not declare requested prefix for %q", contract.ErrInvalidConfiguration, tool)
				}
			} else if permission.Environment != (contract.EnvironmentRequirement{}) || permission.ReadPrefix != "" {
				return fmt.Errorf("%w: unexpected workspace permission for %q", contract.ErrInvalidConfiguration, tool)
			}
		}
		r.permissions[subject] = maps.Clone(access)
	}
	for model, p := range r.contextPolicies {
		if p.Compaction == nil {
			continue
		}
		binding := r.tools[p.Compaction.SourceTool]
		if !binding.Spec.NeedsContextSource || binding.Spec.NeedsReadAccess {
			return fmt.Errorf("%w: invalid context source tool for %q", contract.ErrInvalidConfiguration, model)
		}
	}

	return nil
}

func validReadPrefix(prefix string) bool {
	_, ok := environment.RelativeFile(".", prefix+"/file")
	return ok || prefix == "."
}

func (r *Runner) admitTools(subject contract.SubjectID, req contract.StartRequest) error {
	seen := make(map[contract.ToolRef]struct{}, len(req.Tools))
	needsEnvironment := false
	for _, ref := range req.Tools {
		if _, duplicate := seen[ref]; duplicate {
			return contract.ErrInvalidRequest
		}
		seen[ref] = struct{}{}
		binding, ok := r.tools[ref]
		if !ok {
			return contract.ErrBindingNotFound
		}
		permission, allowed := r.permissions[subject][ref]
		if !allowed {
			return contract.ErrAccessDenied
		}
		if binding.Spec.NeedsContextSource {
			policy := r.contextPolicies[req.Model].Compaction
			if policy == nil || policy.SourceTool != ref {
				return contract.ErrAccessDenied
			}
		}
		if binding.Spec.NeedsReadAccess {
			needsEnvironment = true
			if req.Environment == nil || permission.Environment != *req.Environment {
				return contract.ErrAccessDenied
			}
		}
	}
	if !needsEnvironment && req.Environment != nil {
		return contract.ErrInvalidRequest
	}
	return nil
}

func (r *Runner) modelTools(refs []contract.ToolRef) []contract.ModelTool {
	var tools []contract.ModelTool
	for _, ref := range refs {
		spec := r.tools[ref].Spec
		tools = append(tools, contract.ModelTool{Ref: ref, Description: spec.Description, InputSchema: bytes.Clone(spec.InputSchema)})
	}
	return tools
}

type activity struct {
	transport      mesh.MeshTransport
	env            mesh.EnvironmentRequest
	readCap        uint64
	usedArtifacts  bool
	cleanupPending bool
}

func (r *Runner) newActivity(s *state, subject contract.SubjectID) *activity {
	scope := contract.RunScope{
		Runner: r.ref, Subject: subject, RunID: s.request.ID, Deadline: s.deadline,
	}
	var ref contract.EnvironmentRef
	if requirement := s.request.Environment; requirement != nil {
		scope.Workspace = requirement.Workspace
		ref = requirement.Ref
	}
	return &activity{transport: r.devices, env: mesh.EnvironmentRequest{Ref: ref, Run: scope}}
}

func (a *activity) allocate(s *state) bool {
	if a.env.Ref == "" {
		return true
	}
	described, err := mesh.CallComponent(s.ctx, a.transport, mesh.DescribeEnvironmentRead, a.env, contract.ToolRequest{})
	if err != nil {
		s.failModel(contract.Failed, contract.StopEnvironmentFailure, 0)
		return false
	}
	if caps := described.ReadCapabilities; caps != nil {
		if caps.Scan != contract.CapabilityEnforced || caps.Deadline != contract.CapabilityEnforced ||
			caps.MaxScanBytes == 0 || caps.MaxConcurrentReads <= 0 || caps.MaxQueuedReads < 0 {
			s.failModel(contract.Failed, contract.StopEnvironmentCapability, 0)
			return false
		}
		a.readCap = caps.MaxScanBytes
	}
	reply, err := mesh.CallComponent(s.ctx, a.transport, mesh.AllocateEnvironment, a.env, contract.ToolRequest{})
	if err != nil || reply.Handle == "" {
		s.failModel(contract.Failed, contract.StopEnvironmentFailure, 0)
		return false
	}
	a.env.Handle = reply.Handle
	return true
}

func (a *activity) finish(s *state) {
	var cleanup contract.CleanupStatus
	if a.usedArtifacts {
		cleanup = contract.CleanupReleased
	}
	if a.env.Handle != "" {
		_, err := cleanupCall(a.transport, mesh.ReleaseEnvironment, a.env)
		cleanup = contract.CleanupReleased
		if err != nil {
			cleanup = contract.CleanupPending
		}
	}
	if a.cleanupPending {
		cleanup = contract.CleanupPending
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishFinishedLocked(cleanup)
}

func (r *Runner) toolBatch(s *state, a *activity, balance *limits, assistant *contract.AssistantMessage) bool {
	pending := []contract.Message{{Assistant: assistant}}
	selected := make(map[contract.ToolRef]bool, len(s.request.Tools))
	for _, tool := range s.request.Tools {
		selected[tool] = true
	}
	for _, call := range assistant.ToolCalls {
		s.mu.Lock()
		live := s.runningLocked()
		s.mu.Unlock()
		if !live {
			return false
		}
		result, usable := r.callTool(s, a, balance, call, selected[call.Tool])
		if !usable {
			return false
		}
		pending = append(pending, contract.Message{Tool: &contract.ToolMessage{Tool: call.Tool, Result: result}})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.runningLocked() {
		return false
	}
	s.messages = append(s.messages, pending...)
	return true
}

func (r *Runner) callTool(s *state, a *activity, balance *limits, call contract.ModelToolCall, allowed bool) (contract.ToolResult, bool) {
	if balance.toolRequests == 0 {
		s.failTool(contract.StopToolRequestLimit, call.CallID, s.budget.toolRequests)
		return contract.ToolResult{}, false
	}
	balance.toolRequests--
	binding := r.tools[call.Tool]
	req := contract.ToolRequest{
		RunID: s.request.ID, CallID: call.CallID, Tool: call.Tool, Arguments: bytes.Clone(call.Arguments),
		Limits: contract.ToolLimits{MaxResultBytes: balance.output},
	}
	if allowed {
		req.Limits.MaxScanBytes = min(balance.scan, binding.Spec.Limits.MaxScanBytes)
		if binding.Spec.NeedsReadAccess && a.readCap > 0 {
			req.Limits.MaxScanBytes = min(req.Limits.MaxScanBytes, a.readCap)
		}
		req.Limits.MaxResultBytes = min(balance.output, binding.Spec.Limits.MaxResultBytes)
	}
	if req.Limits.MaxResultBytes < minimumReceiptSize(req) {
		s.failTool(contract.StopToolReceiptLimit, call.CallID, req.Limits.MaxResultBytes)
		return contract.ToolResult{}, false
	}
	reject := func(code string) (contract.ToolResult, bool) {
		result := skippedTool(req, code)
		return result, s.settleTool(balance, req, result, contract.ToolStatusRejected, false, 0, true, 0)
	}
	if !allowed {
		return reject("tool_not_authorized")
	}
	if binding.Spec.NeedsArtifactOutput {
		req.Limits.MaxRawOutputBytes = min(balance.raw, binding.Spec.Limits.MaxRawOutputBytes)
		if req.Limits.MaxRawOutputBytes == 0 {
			s.failTool(contract.StopToolRawOutputLimit, call.CallID, s.budget.raw)
			return contract.ToolResult{}, false
		}
	}
	e := a.env
	e.Call = contract.CallScope{Run: e.Run, CallID: call.CallID, Tool: call.Tool, Handle: e.Handle, Limits: req.Limits}
	if binding.Prepare != nil {
		file, err := binding.Prepare(bytes.Clone(call.Arguments))
		if err != nil {
			return reject("invalid_arguments")
		}
		e.Call.Path = file
	}
	if binding.Spec.NeedsReadAccess {
		permission := r.permissions[e.Run.Subject][req.Tool]
		if _, ok := environment.RelativeFile(permission.ReadPrefix, e.Call.Path); !ok {
			return reject("path_not_authorized")
		}
		if req.Limits.MaxScanBytes == 0 {
			s.failTool(contract.StopToolScanLimit, call.CallID, s.budget.scan)
			return contract.ToolResult{}, false
		}
		e.Prefix = permission.ReadPrefix
		grant, err := mesh.CallComponent(s.ctx, a.transport, mesh.GrantEnvironment, e, contract.ToolRequest{})
		if errors.Is(err, contract.ErrEnvironmentBusy) {
			return reject("environment_busy")
		}
		if err != nil || grant.Grant == "" {
			return reject("grant_denied")
		}
		e.Grant, req.EnvironmentGrant = grant.Grant, grant.Grant
	}

	if binding.Spec.NeedsArtifactOutput {
		reply, err := mesh.CallComponent(s.ctx, a.transport, mesh.BeginArtifactOutput, e, contract.ToolRequest{})
		if err != nil || reply.RawOutputBytes == 0 || reply.RawOutputBytes > req.Limits.MaxRawOutputBytes {
			if e.Grant != "" {
				_, _ = cleanupCall(a.transport, mesh.RevokeEnvironment, e)
			}
			return reject("artifact_capacity")
		}
		e.Call.Limits.MaxRawOutputBytes, req.Limits.MaxRawOutputBytes = reply.RawOutputBytes, reply.RawOutputBytes
		a.usedArtifacts = true
	}

	s.mu.Lock()
	if !s.runningLocked() {
		s.mu.Unlock()
		if e.Grant != "" {
			_, _ = cleanupCall(a.transport, mesh.RevokeEnvironment, e)
		}
		if binding.Spec.NeedsArtifactOutput {
			if _, err := cleanupCall(a.transport, mesh.SettleArtifactOutput, e); err != nil {
				a.cleanupPending = true
			}
		}
		return contract.ToolResult{}, false
	}
	s.activeCall = req.CallID
	s.events.publish(contract.Event{
		Kind: contract.EventToolDispatched, Step: s.step, Tool: &contract.ToolEvent{CallID: req.CallID, Tool: req.Tool},
	})
	s.mu.Unlock()

	invokeCtx := s.ctx
	if binding.Spec.NeedsContextSource {
		invokeCtx = contract.WithContextSource(invokeCtx, invocationSource{state: s, call: req.CallID})
	}
	result := invoke(invokeCtx, a.transport, e, req)
	var measured uint64
	verified := e.Grant == ""
	if e.Grant != "" {
		meter, err := cleanupCall(a.transport, mesh.RevokeEnvironment, e)
		measured, verified = meter.Scanned, err == nil && meter.Scanned <= req.Limits.MaxScanBytes
	}
	var raw uint64
	if binding.Spec.NeedsArtifactOutput {
		result, raw = a.settleArtifact(e, req, result)
	} else if result.Completion != nil && len(result.Completion.Artifacts) != 0 ||
		result.Usage != nil && result.Usage.RawOutputBytes != nil {
		result = contract.ToolResult{}
	}
	if (verified && measured > 0) && result.State == contract.ToolExecutionNotExecuted {
		result = contract.ToolResult{}
	}
	if verified && e.Grant != "" && validToolResult(result, req.RunID, req.CallID) {
		if result.Usage == nil {
			result.Usage = &contract.ToolUsage{}
		}
		result.Usage.ScannedBytes = &measured
	}
	return result, s.settleTool(balance, req, result, toolStatus(result), true, measured, verified, raw)
}

func (a *activity) settleArtifact(e mesh.EnvironmentRequest, req contract.ToolRequest, result contract.ToolResult) (contract.ToolResult, uint64) {
	if validToolResult(result, req.RunID, req.CallID) && result.State == contract.ToolExecutionKnown {
		e.Artifacts = result.Completion.Artifacts
	}
	reply, err := cleanupCall(a.transport, mesh.SettleArtifactOutput, e)
	if err != nil && (!errors.Is(err, contract.ErrArtifactReceipt) || errors.Is(err, contract.ErrArtifactCleanup)) {
		a.cleanupPending = true
	}
	if err != nil || reply.RawOutputBytes > req.Limits.MaxRawOutputBytes {
		used := req.Limits.MaxRawOutputBytes
		if errors.Is(err, contract.ErrArtifactReceipt) && reply.RawOutputBytes <= used {
			used = reply.RawOutputBytes
		}
		if len(e.Artifacts) != 0 {
			return contract.ToolResult{}, used
		}
		return result, used
	}
	if reply.RawOutputBytes != 0 && result.State == contract.ToolExecutionNotExecuted {
		return contract.ToolResult{}, reply.RawOutputBytes
	}
	if validToolResult(result, req.RunID, req.CallID) {
		if result.Usage == nil {
			result.Usage = &contract.ToolUsage{}
		}
		result.Usage.RawOutputBytes = &reply.RawOutputBytes
	}
	return result, reply.RawOutputBytes
}

type componentReply struct {
	reply mesh.MeshReply
	err   error
}

func invoke(ctx context.Context, transport mesh.MeshTransport, e mesh.EnvironmentRequest, req contract.ToolRequest) contract.ToolResult {
	results := make(chan componentReply, 1)
	go func() {
		reply, err := mesh.CallComponent(ctx, transport, mesh.Invoke, e, req)
		results <- componentReply{reply, err}
	}()
	var response componentReply
	select {
	case response = <-results:
	case <-ctx.Done():
		timer := time.NewTimer(receiptGrace)
		defer timer.Stop()
		select {
		case response = <-results:
		case <-timer.C:
			return contract.ToolResult{}
		}
	}
	if errors.Is(response.err, contract.ErrNotDelivered) {
		return skippedTool(req, "request_not_delivered")
	}
	if response.err != nil {
		return contract.ToolResult{}
	}
	return cloneToolResult(response.reply.Tool)
}

func cleanupCall(transport mesh.MeshTransport, op mesh.Operation, e mesh.EnvironmentRequest) (mesh.MeshReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupGrace)
	defer cancel()
	reply := make(chan componentReply, 1)
	go func() {
		result, err := mesh.CallComponent(ctx, transport, op, e, contract.ToolRequest{})
		reply <- componentReply{result, err}
	}()
	select {
	case result := <-reply:
		return result.reply, result.err
	case <-ctx.Done():
		return mesh.MeshReply{}, ctx.Err()
	}
}

func (s *state) settleTool(balance *limits, req contract.ToolRequest, result contract.ToolResult, status contract.ToolStatus,
	dispatched bool, measured uint64, verified bool, raw uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.runningLocked()
	if !dispatched && !live {
		return false
	}
	s.activeCall = ""
	if !verified {
		measured = req.Limits.MaxScanBytes
	}
	balance.scan -= measured
	balance.raw -= raw
	var code string
	if !validToolResult(result, req.RunID, req.CallID) {
		status = contract.ToolStatusUnknown
	} else if result.Reason != nil {
		code = result.Reason.Code
	}
	s.events.publish(contract.Event{Kind: contract.EventToolCompleted, Step: s.step,
		Tool: &contract.ToolEvent{CallID: req.CallID, Tool: req.Tool, Status: status, Code: code}})
	if status == contract.ToolStatusUnknown {
		s.finishLocked(contract.ToolOutcomeUnknown, nil, s.usage, &contract.StopDetail{
			Code: contract.StopToolUnknown, Step: s.step, CallID: req.CallID,
		})
		return false
	}
	if !live {
		s.finishLocked(s.stopOutcome, nil, s.usage, s.stop)
		return false
	}
	size, fits := receiptSize(result, req.Limits.MaxResultBytes)
	if !fits {
		s.finishLocked(contract.BudgetExceeded, nil, s.usage, &contract.StopDetail{
			Code: contract.StopToolReceiptLimit, Step: s.step, CallID: req.CallID, LimitBytes: req.Limits.MaxResultBytes,
		})
		return false
	}
	balance.output -= size
	return true
}

func (s *state) failTool(code contract.StopCode, call contract.CallID, limit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningLocked() {
		s.finishLocked(contract.BudgetExceeded, nil, s.usage, &contract.StopDetail{
			Code: code, Step: s.step, CallID: call, LimitBytes: limit,
		})
	}
}

func skippedTool(req contract.ToolRequest, code string) contract.ToolResult {
	zero := uint64(0)
	return contract.ToolResult{
		RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted,
		Reason: &contract.ToolReason{Code: code}, Usage: &contract.ToolUsage{ScannedBytes: &zero},
	}
}

func minimumReceiptSize(req contract.ToolRequest) uint64 {
	skipped := skippedTool(req, "request_not_delivered")
	size, _ := receiptSize(skipped, ^uint64(0))
	return size
}

func receiptSize(result contract.ToolResult, limit uint64) (uint64, bool) {
	if result.Completion != nil && uint64(len(result.Completion.Content)) > limit {
		return 0, false
	}
	encoded, _ := json.Marshal(result)
	size := uint64(len(encoded))
	return size, size <= limit
}

func validToolResult(result contract.ToolResult, run contract.RunID, call contract.CallID) bool {
	if run == "" || result.RunID != run || result.CallID != call ||
		(result.Reason != nil && (len(result.Reason.Code) == 0 || len(result.Reason.Code) > 64 || !utf8.ValidString(result.Reason.Code) ||
			len(result.Reason.Message) > 256 || !utf8.ValidString(result.Reason.Message))) {
		return false
	}
	switch result.State {
	case contract.ToolExecutionKnown:
		if result.Completion == nil || len(result.Completion.Artifacts) > 1 || !utf8.ValidString(result.Completion.Content) {
			return false
		}
		for _, a := range result.Completion.Artifacts {
			if a.ID == "" || len(a.ID) > 128 || a.Version == "" || len(a.Version) > 128 || len(a.SHA256) != 64 || a.ExpiresAt.IsZero() || a.ExpiresAt.Year() > 9999 || a.ExpiresAt.Year() < 0 {
				return false
			}
		}
		switch result.Completion.Outcome {
		case contract.ToolSucceeded:
			return result.Reason == nil
		case contract.ToolFailed, contract.ToolCancelled:
			return result.Reason != nil
		}
	case contract.ToolExecutionNotExecuted, contract.ToolExecutionUnknown:
		return result.Completion == nil && result.Reason != nil
	}
	return false
}

func toolStatus(result contract.ToolResult) contract.ToolStatus {
	switch result.State {
	case contract.ToolExecutionKnown:
		if result.Completion != nil {
			switch result.Completion.Outcome {
			case contract.ToolSucceeded:
				return contract.ToolStatusSucceeded
			case contract.ToolCancelled:
				return contract.ToolStatusCancelled
			}
		}
		return contract.ToolStatusFailed
	case contract.ToolExecutionNotExecuted:
		return contract.ToolStatusFailed
	default:
		return contract.ToolStatusUnknown
	}
}
