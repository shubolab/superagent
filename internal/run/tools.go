package run

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/remotetool"
)

const (
	receiptGrace = 200 * time.Millisecond
	cleanupGrace = 200 * time.Millisecond
)

func (r *Runner) ConfigureTools(ref contract.RunnerRef, transport mesh.MeshTransport, cfg contract.RunnerConfig,
	tools map[contract.ToolRef]contract.LocalToolBinding, environments map[contract.EnvironmentRef]contract.EnvironmentProvider, artifactStore bool) error {
	r.ref, r.devices = ref, transport
	r.takeoverEnvironments = make(map[contract.EnvironmentRef]contract.TakeoverWorkspace)
	for ref, env := range environments {
		if capable, ok := env.(contract.TakeoverWorkspace); ok {
			r.takeoverEnvironments[ref] = capable
		}
	}
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
			if binding.Spec.NeedsWorkspaceWrite != permission.AllowWorkspaceWrite {
				return fmt.Errorf("%w: invalid workspace write permission for %q", contract.ErrInvalidConfiguration, tool)
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
		if _, remote := binding.Tool.(*remotetool.Client); remote {
			if !req.Recoverable || binding.Spec.NeedsReadAccess || binding.Spec.NeedsWorkspaceWrite ||
				binding.Spec.NeedsArtifactOutput || binding.Spec.NeedsContextSource {
				return contract.ErrRecoveryUnavailable
			}
		}
		permission, allowed := r.permissions[subject][ref]
		if !allowed {
			return contract.ErrAccessDenied
		}
		if binding.Spec.NeedsWorkspaceWrite && !req.Takeover {
			return contract.ErrRecoveryUnavailable
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
	guard          mesh.TakeoverGuard
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
	a := &activity{transport: r.devices}
	if s.checkpoint != nil && s.checkpoint.lease != nil {
		a.guard = s.checkpoint.lease
		scope.Epoch = s.checkpoint.lease.owner.Epoch
	}
	a.env = mesh.EnvironmentRequest{Ref: ref, Run: scope}
	return a
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
		_, err := a.cleanupCall(mesh.ReleaseEnvironment, a.env, contract.ReceiptKey{})
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
	if d := s.checkpoint; d != nil && (d.data.RemoteCleanup == contract.CleanupPending || cleanup == "") {
		cleanup = d.data.RemoteCleanup
	}
	s.publishFinishedLocked(cleanup)
}

func (r *Runner) toolBatch(s *state, a *activity, balance *limits, assistant *contract.AssistantMessage) bool {
	pending := []contract.Message{{Assistant: assistant}}
	selected := make(map[contract.ToolRef]bool, len(s.request.Tools))
	for _, tool := range s.request.Tools {
		selected[tool] = true
	}
	for i, call := range assistant.ToolCalls {
		if d := s.checkpoint; d != nil && i < len(d.data.ToolResults) {
			result := d.data.ToolResults[i]
			pending = append(pending, contract.Message{Tool: &contract.ToolMessage{Tool: call.Tool, Result: result}})
			continue
		}
		s.mu.Lock()
		live := s.runningLocked()
		s.mu.Unlock()
		if !live {
			return false
		}
		allowed := selected[call.Tool]
		if s.checkpoint != nil {
			_, currentlyAllowed := r.permissions[a.env.Run.Subject][call.Tool]
			allowed = allowed && currentlyAllowed
		}
		result, usable := r.callTool(s, a, balance, call, allowed)
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
	if d := s.checkpoint; d != nil {
		d.data.Pending, d.data.ToolResults = nil, nil
		d.data.Step, d.data.Balance = s.step+1, *balance
		return s.saveLocked()
	}
	return true
}

func (r *Runner) callTool(s *state, a *activity, balance *limits, call contract.ModelToolCall, allowed bool) (contract.ToolResult, bool) {
	if balance.ToolRequests == 0 {
		s.failTool(contract.StopToolRequestLimit, call.CallID, s.budget.ToolRequests)
		return contract.ToolResult{}, false
	}
	balance.ToolRequests--
	binding := r.tools[call.Tool]
	req := contract.ToolRequest{
		RunID: s.request.ID, CallID: call.CallID, Tool: call.Tool, Arguments: bytes.Clone(call.Arguments),
		Limits: contract.ToolLimits{MaxResultBytes: balance.Output},
	}
	if allowed {
		req.Limits.MaxScanBytes = min(balance.Scan, binding.Spec.Limits.MaxScanBytes)
		if binding.Spec.NeedsReadAccess && a.readCap > 0 {
			req.Limits.MaxScanBytes = min(req.Limits.MaxScanBytes, a.readCap)
		}
		req.Limits.MaxResultBytes = min(balance.Output, binding.Spec.Limits.MaxResultBytes)
	}
	if req.Limits.MaxResultBytes < minimumReceiptSize(req) {
		s.failTool(contract.StopToolReceiptLimit, call.CallID, req.Limits.MaxResultBytes)
		return contract.ToolResult{}, false
	}
	reject := func(code string) (contract.ToolResult, bool) {
		result := skippedTool(req, code)
		return result, s.settleTool(balance, req, result, contract.ToolStatusRejected, false, 0, true, 0, nil)
	}
	if !allowed {
		return reject("tool_not_authorized")
	}
	if binding.Spec.NeedsArtifactOutput {
		req.Limits.MaxRawOutputBytes = min(balance.Raw, binding.Spec.Limits.MaxRawOutputBytes)
		if req.Limits.MaxRawOutputBytes == 0 {
			s.failTool(contract.StopToolRawOutputLimit, call.CallID, s.budget.Raw)
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
	current, original := r.recoveryPermission(s, e.Run.Subject, req.Tool)
	if (binding.Spec.NeedsWorkspaceWrite && !original.AllowWorkspaceWrite) ||
		(s.request.Takeover && binding.Spec.NeedsArtifactOutput && !original.AllowArtifacts) {
		return reject("tool_not_authorized")
	}
	if binding.Spec.NeedsReadAccess {
		_, allowedNow := environment.RelativeFile(current.ReadPrefix, e.Call.Path)
		_, allowedBefore := environment.RelativeFile(original.ReadPrefix, e.Call.Path)
		if !allowedNow || !allowedBefore || original.Environment != current.Environment {
			return reject("path_not_authorized")
		}
		if req.Limits.MaxScanBytes == 0 {
			s.failTool(contract.StopToolScanLimit, call.CallID, s.budget.Scan)
			return contract.ToolResult{}, false
		}
		e.Prefix = current.ReadPrefix
	}
	if s.request.Takeover {
		s.mu.Lock()
		saved := s.runningLocked() && s.recordToolIntent(e, req, *balance)
		s.mu.Unlock()
		if !saved {
			return contract.ToolResult{}, false
		}
	}
	if binding.Spec.NeedsReadAccess {
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
				_, _ = a.cleanupCall(mesh.RevokeEnvironment, e, contract.ReceiptKey{})
			}
			return reject("artifact_capacity")
		}
		e.Call.Limits.MaxRawOutputBytes, req.Limits.MaxRawOutputBytes = reply.RawOutputBytes, reply.RawOutputBytes
		a.usedArtifacts = true
	}

	s.mu.Lock()
	if !s.runningLocked() || !s.recordToolIntent(e, req, *balance) {
		s.mu.Unlock()
		if e.Grant != "" {
			_, _ = a.cleanupCall(mesh.RevokeEnvironment, e, contract.ReceiptKey{})
		}
		if binding.Spec.NeedsArtifactOutput {
			if _, err := a.cleanupCall(mesh.SettleArtifactOutput, e, contract.ReceiptKey{}); err != nil {
				a.cleanupPending = true
			}
		}
		return contract.ToolResult{}, false
	}
	s.activeCall = req.CallID
	s.events.publish(contract.Event{
		Kind: contract.EventToolDispatched, Step: s.step, Tool: &contract.ToolEvent{CallID: req.CallID, Tool: req.Tool},
	})
	var key contract.ReceiptKey
	if d := s.checkpoint; d != nil {
		key = d.data.Intent.Key
	}
	s.mu.Unlock()

	invokeCtx := s.ctx
	if binding.Spec.NeedsContextSource {
		invokeCtx = contract.WithContextSource(invokeCtx, invocationSource{state: s, call: req.CallID})
	}
	result, remoteCleanup := invoke(invokeCtx, a.transport, e, req, key)
	var measured uint64
	verified := e.Grant == ""
	if e.Grant != "" {
		meter, err := a.cleanupCall(mesh.RevokeEnvironment, e, contract.ReceiptKey{})
		measured, verified = meter.Scanned, err == nil && meter.Scanned <= req.Limits.MaxScanBytes
	}
	if key.Remote != (contract.RemoteToolIdentity{}) {
		verified = result.State == contract.ToolExecutionNotExecuted
		if result.Usage != nil && result.Usage.ScannedBytes != nil {
			measured = *result.Usage.ScannedBytes
			verified = measured <= req.Limits.MaxScanBytes
			if !verified {
				result = contract.ToolResult{}
			}
		}
	}
	if (verified && measured > 0) && result.State == contract.ToolExecutionNotExecuted {
		result = contract.ToolResult{}
	}
	if verified && e.Grant != "" && contract.ValidToolResult(result, req.RunID, req.CallID) {
		if result.Usage == nil {
			result.Usage = &contract.ToolUsage{}
		}
		result.Usage.ScannedBytes = &measured
	}
	var raw uint64
	if binding.Spec.NeedsArtifactOutput {
		durable := r.artifactStoreID != ""
		if durable && key != (contract.ReceiptKey{}) && (!contract.ValidToolResult(result, req.RunID, req.CallID) || toolStatus(result) == contract.ToolStatusUnknown) {
			// An ambiguous reply may have a durable receipt selecting the sealed object.
			return r.reconcileTool(s, a, balance, toolIntent{Key: key, Limits: req.Limits, Path: e.Call.Path})
		}
		result, raw = a.settleArtifact(s.ctx, e, req, result, key, durable)
	} else if result.Completion != nil && len(result.Completion.Artifacts) != 0 ||
		result.Usage != nil && result.Usage.RawOutputBytes != nil {
		result = contract.ToolResult{}
	}
	if key != (contract.ReceiptKey{}) && (!contract.ValidToolResult(result, req.RunID, req.CallID) || toolStatus(result) == contract.ToolStatusUnknown) {
		return r.reconcileTool(s, a, balance, toolIntent{Key: key, Limits: req.Limits, Path: e.Call.Path})
	}
	if key.Remote != (contract.RemoteToolIdentity{}) {
		s.recordRemoteCleanup(remoteCleanup)
	}
	ok := s.settleTool(balance, req, result, toolStatus(result), true, measured, verified, raw, nil)
	if ok && key.Remote != (contract.RemoteToolIdentity{}) && result.State != contract.ToolExecutionNotExecuted {
		ok = r.ensureRemoteWorkspace(s, a, key.Tool, key.Remote)
	}
	return result, ok
}

func (a *activity) settleArtifact(ctx context.Context, e mesh.EnvironmentRequest, req contract.ToolRequest, result contract.ToolResult, key contract.ReceiptKey, durable bool) (contract.ToolResult, uint64) {
	if contract.ValidToolResult(result, req.RunID, req.CallID) && result.State == contract.ToolExecutionKnown {
		e.Artifacts = result.Completion.Artifacts
	}
	var reply mesh.MeshReply
	var err error
	if durable && len(e.Artifacts) != 0 {
		used, fits := meterRetainedResult(&result, req.Limits)
		if !fits {
			a.cleanupPending = true
			return contract.ToolResult{}, req.Limits.MaxRawOutputBytes
		}
		reply, err = mesh.CallReceipt(ctx, a.transport, mesh.SettleArtifactOutput, e, req, key)
		if err == nil && reply.RawOutputBytes != used {
			err = contract.ErrArtifactReceipt
		}
	} else {
		reply, err = a.cleanupCall(mesh.SettleArtifactOutput, e, key)
	}
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
	if contract.ValidToolResult(result, req.RunID, req.CallID) {
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

func invoke(ctx context.Context, transport mesh.MeshTransport, e mesh.EnvironmentRequest, req contract.ToolRequest, key contract.ReceiptKey) (contract.ToolResult, contract.CleanupStatus) {
	results := make(chan componentReply, 1)
	go func() {
		reply, err := mesh.CallReceipt(ctx, transport, mesh.Invoke, e, req, key)
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
			return contract.ToolResult{}, ""
		}
	}
	if errors.Is(response.err, contract.ErrNotDelivered) {
		return skippedTool(req, "request_not_delivered"), contract.CleanupReleased
	}
	if response.err != nil {
		return contract.ToolResult{}, ""
	}
	return cloneToolResult(response.reply.Tool), response.reply.Cleanup
}

func (a *activity) cleanupCall(op mesh.Operation, e mesh.EnvironmentRequest, key contract.ReceiptKey) (mesh.MeshReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupGrace)
	defer cancel()
	if a.guard != nil {
		ctx = mesh.WithTakeoverGuard(ctx, a.guard)
	}
	reply := make(chan componentReply, 1)
	go func() {
		result, err := mesh.CallReceipt(ctx, a.transport, op, e, contract.ToolRequest{}, key)
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
	dispatched bool, measured uint64, verified bool, raw uint64, evidence *contract.WriteEvidenceRef) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.runningLocked()
	if s.checkpointErr != nil || (!dispatched && !live) {
		return false
	}
	s.activeCall = ""
	if !verified {
		measured = req.Limits.MaxScanBytes
	}
	balance.Scan -= measured
	balance.Raw -= raw
	var code string
	if !contract.ValidToolResult(result, req.RunID, req.CallID) {
		status = contract.ToolStatusUnknown
	} else if result.Reason != nil {
		code = result.Reason.Code
	}
	event := contract.Event{Kind: contract.EventToolCompleted, Step: s.step,
		Tool: &contract.ToolEvent{CallID: req.CallID, Tool: req.Tool, Status: status, Code: code}}
	if !s.request.Takeover {
		s.events.publish(event)
	}
	if status == contract.ToolStatusUnknown {
		stop := &contract.StopDetail{Code: contract.StopToolUnknown, Step: s.step, CallID: req.CallID}
		if d := s.checkpoint; d != nil {
			balance.Output -= req.Limits.MaxResultBytes
			d.data.Intent, d.data.Balance = nil, *balance
			stop.Trigger = contract.StopReconcileDeadline
			if s.stop != nil {
				stop.Trigger = s.stop.Code
			}
		}
		s.finishLocked(contract.ToolOutcomeUnknown, nil, s.usage, stop)
		return false
	}
	if !live && s.checkpoint == nil {
		s.finishLocked(s.stopOutcome, nil, s.usage, s.stop)
		return false
	}
	size, fits := receiptSize(result, req.Limits.MaxResultBytes)
	if !fits {
		if d := s.checkpoint; d != nil {
			balance.Output -= req.Limits.MaxResultBytes
			d.data.Intent, d.data.Balance = nil, *balance
		}
		if !live {
			s.finishLocked(s.stopOutcome, nil, s.usage, s.stop)
			return false
		}
		s.finishLocked(contract.BudgetExceeded, nil, s.usage, &contract.StopDetail{
			Code: contract.StopToolReceiptLimit, Step: s.step, CallID: req.CallID, LimitBytes: req.Limits.MaxResultBytes,
		})
		return false
	}
	balance.Output -= size
	if d := s.checkpoint; d != nil {
		d.data.ToolResults = append(d.data.ToolResults, cloneToolResult(result))
		if evidence != nil {
			if d.data.WriteEvidence == nil {
				d.data.WriteEvidence = make(map[contract.CallID]contract.WriteEvidenceRef)
			}
			d.data.WriteEvidence[req.CallID] = *evidence
		}
		d.data.Intent, d.data.Balance = nil, *balance
		if !s.saveLocked() {
			return false
		}
		if s.request.Takeover {
			s.events.publish(event)
		}
	}
	if !live {
		s.finishLocked(s.stopOutcome, nil, s.usage, s.stop)
		return false
	}
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
