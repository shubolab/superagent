package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/remotetool"
)

type runIdentity struct {
	Runner  contract.RunnerRef
	Subject contract.SubjectID
	ID      contract.RunID
}

type toolIntent struct {
	Key      contract.ReceiptKey
	Limits   contract.ToolLimits
	Path     string
	Admitted bool `json:",omitempty"`
}

type checkpoint struct {
	Format           int
	Identity         runIdentity
	BindingVersion   string
	Request          contract.StartRequest
	RequestDigest    string
	Permissions      map[contract.ToolRef]contract.ToolPermission
	WorkspaceVersion string
	RemoteBindings   map[contract.ToolRef]contract.RemoteToolIdentity `json:",omitempty"`
	WriteEvidence    map[contract.CallID]contract.WriteEvidenceRef    `json:",omitempty"`
	RemoteCleanup    contract.CleanupStatus                           `json:",omitempty"`
	Deadline         time.Time
	ReconcileTimeout time.Duration
	ReconcileUntil   time.Time
	Budget, Balance  limits
	Step             uint32
	ModelInFlight    bool
	ModelAdmitted    bool       `json:",omitempty"`
	Owner            *ownership `json:",omitempty"`
	Pending          *contract.AssistantMessage
	ToolResults      []contract.ToolResult
	Intent           *toolIntent
	Messages         []contract.Message
	Usage            *contract.Usage
	StopOutcome      contract.RunOutcome
	Stop             *contract.StopDetail
	Result           *contract.Result
	FinishedAt       time.Time
}

type runCheckpoint struct {
	data    checkpoint
	path    string
	lock    *os.File
	resumed bool
	lease   *lease
}

func (r *Runner) recoveryPath(subject contract.SubjectID, id contract.RunID) (string, runIdentity) {
	key := runIdentity{Runner: r.ref, Subject: subject, ID: id}
	digest, _ := durable.Digest(key)
	return filepath.Join(r.recovery.Directory, digest+".run"), key
}

func readCheckpoint(path string, id runIdentity) (checkpoint, error) {
	var c checkpoint
	if err := durable.Read(path, &c); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return c, contract.ErrRunNotFound
		}
		return c, fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
	}
	if c.Format != 1 || c.Identity != id || c.Request.ID != id.ID || !c.Request.Recoverable ||
		c.Deadline.IsZero() || c.BindingVersion == "" || c.ReconcileTimeout <= 0 ||
		!validHistory(c.Messages) || len(c.Messages) == 0 || c.Messages[0].User == nil || *c.Messages[0].User != c.Request.Input ||
		!c.validBalance() || !c.validOwner() ||
		(c.Step == 0 && c.Result == nil) || (c.Result == nil) != c.FinishedAt.IsZero() ||
		(c.Result != nil && (c.Result.RunID != id.ID || c.Result.Outcome == "")) ||
		(c.Stop == nil) != (c.StopOutcome == "") || !c.validPosition() {
		return c, contract.ErrRecoveryStorage
	}
	digest, err := durable.Digest(c.Request)
	if err != nil || c.RequestDigest != digest {
		return c, contract.ErrRecoveryStorage
	}
	return c, nil
}

func (c checkpoint) validOwner() bool {
	if !c.Request.Takeover {
		return c.Owner == nil
	}
	return c.Owner != nil && c.Owner.Instance != "" && c.Owner.Epoch > 0 && c.Owner.Revision > 0 &&
		c.Owner.Duration > 0 && !c.Owner.Expires.IsZero()
}

func (c checkpoint) validBalance() bool {
	initial, left := c.Budget, c.Balance
	return initial.Timeout > 0 && initial.ModelRequests > 0 && initial.ModelInput > 0 && initial.Output > 0 &&
		left.Timeout == initial.Timeout && left.ModelRequests <= initial.ModelRequests && left.ModelInput <= initial.ModelInput &&
		left.Output <= initial.Output && left.ToolRequests <= initial.ToolRequests && left.Scan <= initial.Scan && left.Raw <= initial.Raw
}

func (c checkpoint) validPosition() bool {
	if c.Pending == nil {
		return c.Intent == nil && len(c.ToolResults) == 0
	}
	if c.ModelInFlight || !validCalls(c.Pending.ToolCalls, nil) || len(c.Pending.ToolCalls) == 0 ||
		len(c.ToolResults) > len(c.Pending.ToolCalls) {
		return false
	}
	for i, result := range c.ToolResults {
		if !contract.ValidToolResult(result, c.Identity.ID, c.Pending.ToolCalls[i].CallID) || toolStatus(result) == contract.ToolStatusUnknown {
			return false
		}
	}
	if c.Intent == nil {
		return true
	}
	if len(c.ToolResults) == len(c.Pending.ToolCalls) {
		return false
	}
	key, limits := c.Intent.Key, c.Intent.Limits
	call := c.Pending.ToolCalls[len(c.ToolResults)]
	environment := valueOr(c.Request.Environment, contract.EnvironmentRequirement{})
	request := contract.ToolRequest{Tool: call.Tool, Arguments: call.Arguments, Limits: limits}
	return key.RunID == c.Identity.ID && key.Runner == c.Identity.Runner && key.Subject == c.Identity.Subject &&
		key.Step == c.Step && key.CallID == call.CallID && key.Tool == call.Tool &&
		key.BindingVersion == c.BindingVersion && key.WorkspaceVersion == c.WorkspaceVersion &&
		key.Environment == environment.Ref && key.Workspace == environment.Workspace &&
		key.Fingerprint == durable.ToolFingerprint(request, c.Intent.Path) && key.Remote == c.RemoteBindings[call.Tool] &&
		((c.Owner == nil && key.AdmissionEpoch == 0) || (c.Owner != nil && key.AdmissionEpoch > 0 && key.AdmissionEpoch <= c.Owner.Epoch)) &&
		limits.MaxResultBytes > 0 && limits.MaxResultBytes <= c.Balance.Output && limits.MaxScanBytes <= c.Balance.Scan && limits.MaxRawOutputBytes <= c.Balance.Raw
}

func recoveryLock(path string) (*os.File, error) {
	lock, err := durable.Lock(path)
	if errors.Is(err, durable.ErrLocked) {
		return nil, contract.ErrRecoveryBusy
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
	}
	return lock, nil
}

func (r *Runner) remoteBindings(req contract.StartRequest) map[contract.ToolRef]contract.RemoteToolIdentity {
	var bindings map[contract.ToolRef]contract.RemoteToolIdentity
	for _, ref := range req.Tools {
		if remote, ok := r.tools[ref].Tool.(*remotetool.Client); ok {
			if bindings == nil {
				bindings = make(map[contract.ToolRef]contract.RemoteToolIdentity)
			}
			bindings[ref] = remote.Identity()
		}
	}
	return bindings
}

func (r *Runner) validateRemoteBindings(c checkpoint) error {
	if !maps.Equal(c.RemoteBindings, r.remoteBindings(c.Request)) {
		return contract.ErrRecoveryBinding
	}
	return nil
}

func (r *Runner) validateRecovery(subject contract.SubjectID, req contract.StartRequest) error {
	if r.recovery == nil || r.contextPolicies[req.Model].Compaction != nil {
		return contract.ErrRecoveryUnavailable
	}
	if req.Takeover {
		if r.recovery.Takeover == nil {
			return contract.ErrRecoveryUnavailable
		}
		if e := req.Environment; e != nil {
			env := r.takeoverEnvironments[e.Ref]
			if env == nil || !env.FencesWorkspace(e.Workspace) {
				return contract.ErrRecoveryUnavailable
			}
		}
	}
	if _, ok := r.allowed[subject][req.Model]; !ok {
		return contract.ErrAccessDenied
	}
	if err := r.admitTools(subject, req); err != nil {
		return err
	}
	for _, tool := range req.Tools {
		binding := r.tools[tool]
		if _, ok := binding.Tool.(contract.ReceiptProvider); !ok ||
			(binding.Spec.NeedsArtifactOutput && (!r.retainsArtifacts || (req.Takeover && !r.takeoverArtifacts))) {
			return contract.ErrRecoveryUnavailable
		}
	}
	return nil
}

func (r *Runner) startRecoverable(ctx context.Context, subject contract.SubjectID, req contract.StartRequest, budget limits, now time.Time) (contract.RunID, error) {
	if r.recovery == nil {
		return "", contract.ErrRecoveryUnavailable
	}
	path, identity := r.recoveryPath(subject, req.ID)
	digest, err := durable.Digest(req)
	if err != nil {
		return "", contract.ErrInvalidRequest
	}
	// Records are atomically replaced, so duplicate admission can read while the owner runs.
	checkExisting := func() (contract.RunID, error) {
		record, err := readCheckpoint(path, identity)
		if err != nil {
			return "", err
		}
		if record.RequestDigest != digest {
			return "", contract.ErrStartConflict
		}
		if record.Result != nil && !now.Before(record.FinishedAt.Add(r.retention)) {
			return "", contract.ErrStartConflict
		}
		return req.ID, nil
	}
	if id, err := checkExisting(); !errors.Is(err, contract.ErrRunNotFound) {
		return id, err
	}
	var lock *os.File
	if req.Takeover {
		lock, err = lockTransaction(ctx, path)
	} else {
		lock, err = recoveryLock(path)
	}
	if err != nil {
		if req.Takeover {
			return "", fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
		}
		return "", err
	}
	keep := false
	defer func() {
		if !keep {
			_ = lock.Close()
		}
	}()
	if id, err := checkExisting(); !errors.Is(err, contract.ErrRunNotFound) {
		return id, err
	}
	if err := r.validateRecovery(subject, req); err != nil {
		return "", err
	}
	s := newState(req, budget, now.Add(budget.Timeout), r.observeBufferBytes)
	workspace, err := r.workspaceVersion(ctx, r.newActivity(s, subject))
	if err != nil {
		s.cancel()
		return "", err
	}
	if err := ctx.Err(); err != nil {
		s.cancel()
		return "", err
	}
	permissions := make(map[contract.ToolRef]contract.ToolPermission, len(req.Tools))
	for _, ref := range req.Tools {
		permissions[ref] = r.permissions[subject][ref]
	}
	c := checkpoint{
		Format: 1, Identity: identity, BindingVersion: r.recovery.BindingVersion, Request: req, RequestDigest: digest,
		WorkspaceVersion: workspace, Permissions: permissions, Deadline: s.deadline, RemoteBindings: r.remoteBindings(req),
		ReconcileTimeout: r.recovery.ReconcileTimeout, Budget: budget, Balance: budget, Step: 1, Messages: s.messages,
	}
	var shared *lease
	if req.Takeover {
		authorityNow, err := r.leaseClock()
		if err != nil {
			s.cancel()
			return "", fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
		}
		duration := r.recovery.Takeover.LeaseDuration
		c.Owner = &ownership{Instance: r.instance, Epoch: 1, Revision: 1, Expires: authorityNow.Add(duration), Duration: duration}
		shared = &lease{path: path, id: identity, owner: *c.Owner, clock: r.leaseClock}
	}
	renamed, err := durable.Write(path, c)
	if err != nil {
		s.cancel()
		if renamed {
			return "", contract.ErrStartOutcomeUnknown
		}
		return "", fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
	}
	s.checkpoint = &runCheckpoint{data: c, path: path}
	if shared == nil {
		s.checkpoint.lock = lock
		keep = true
	} else {
		s.attachLease(shared)
	}
	r.runs[runKey{subject: subject, id: req.ID}] = s
	go r.generate(s, subject)
	return req.ID, nil
}

func (r *Runner) Recover(ctx context.Context, subject contract.SubjectID, id contract.RunID) (contract.RunID, error) {
	if r.recovery == nil {
		return "", contract.ErrRecoveryUnavailable
	}
	if subject == "" || id == "" {
		return "", contract.ErrInvalidRequest
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	key := runKey{subject: subject, id: id}
	if s := r.runs[key]; s != nil {
		if s.checkpoint == nil || s.expired(time.Now(), r.retention) {
			return "", contract.ErrRunNotFound
		}
		s.mu.Lock()
		current := s.checkpoint.lease
		if current == nil || !s.finishedAt.IsZero() {
			s.mu.Unlock()
			return id, nil
		}
		if s.checkpointErr == nil {
			err := current.owned(ctx, func(*checkpoint, time.Time) error { return nil })
			if !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
				s.mu.Unlock()
				if err != nil {
					return "", err
				}
				return id, nil
			}
			s.failCheckpointLocked(err)
		}
		s.mu.Unlock()
		delete(r.runs, key)
	}
	path, identity := r.recoveryPath(subject, id)
	record, err := readCheckpoint(path, identity)
	if err != nil {
		return "", err
	}
	var lock *os.File
	var shared *lease
	if record.Result == nil {
		if record.Request.Takeover {
			if r.recovery.Takeover == nil {
				return "", contract.ErrRecoveryUnavailable
			}
			record, shared, err = r.acquireTakeover(ctx, path, identity)
		} else {
			lock, err = recoveryLock(path)
			if err == nil {
				record, err = readCheckpoint(path, identity)
			}
		}
		if err != nil {
			if lock != nil {
				_ = lock.Close()
			}
			return "", err
		}
	}
	keep := false
	defer func() {
		if lock != nil && !keep {
			_ = lock.Close()
		}
	}()
	if record.Result != nil && !time.Now().Before(record.FinishedAt.Add(r.retention)) {
		return "", contract.ErrRunNotFound
	}
	if record.Result == nil && shared == nil {
		if record.BindingVersion != r.recovery.BindingVersion {
			return "", contract.ErrRecoveryBinding
		}
		if err := r.validateRemoteBindings(record); err != nil {
			return "", err
		}
		if err := r.validateRecovery(subject, record.Request); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil && (shared == nil || record.Result != nil) {
		return "", err
	}
	s := newState(record.Request, record.Budget, record.Deadline, r.observeBufferBytes)
	s.step, s.messages, s.usage = record.Step, record.Messages, copyValue(record.Usage)
	s.stopOutcome, s.stop = record.StopOutcome, record.Stop
	s.checkpoint = &runCheckpoint{data: record, path: path, lock: lock, resumed: true}
	if record.Intent != nil {
		s.activeCall = record.Intent.Key.CallID
	}
	r.runs[key] = s
	if record.Result != nil {
		s.result, s.finishedAt = *record.Result, record.FinishedAt
		s.cancel()
		close(s.done)
		return id, nil
	}
	if s.stop != nil {
		s.cancel()
	}
	keep = true
	if shared != nil {
		s.attachLease(shared)
	}
	go r.generate(s, subject)
	return id, nil
}

func (s *state) saveLocked() bool {
	d := s.checkpoint
	if d == nil {
		return true
	}
	if s.checkpointErr != nil {
		return false
	}
	d.data.Messages, d.data.Usage = s.messages, s.usage
	d.data.Stop, d.data.StopOutcome = s.stop, s.stopOutcome
	var err error
	if d.lease == nil {
		_, err = durable.Write(d.path, d.data)
	} else {
		err = d.lease.save(&d.data)
	}
	if err != nil {
		s.failCheckpointLocked(err)
		return false
	}
	return true
}

func (s *state) failCheckpointLocked(err error) {
	if s.checkpointErr != nil {
		return
	}
	s.checkpointErr = errors.Join(contract.ErrRecoveryStorage, err)
	s.cancel()
	s.events.abandon()
	close(s.done)
}

func (r *Runner) workspaceVersion(ctx context.Context, a *activity) (string, error) {
	if a.env.Ref == "" {
		return "", nil
	}
	reply, err := mesh.CallComponent(ctx, a.transport, mesh.VerifyRecoveryWorkspace, a.env, contract.ToolRequest{})
	if err != nil {
		return "", err
	}
	if reply.WorkspaceVersion == "" {
		return "", contract.ErrRecoveryUnavailable
	}
	return reply.WorkspaceVersion, nil
}

func (r *Runner) recoveryPermission(s *state, subject contract.SubjectID, tool contract.ToolRef) (contract.ToolPermission, contract.ToolPermission) {
	current := r.permissions[subject][tool]
	original := current
	if s.checkpoint != nil {
		original = s.checkpoint.data.Permissions[tool]
	}
	return current, original
}

// SetRetainedArtifacts is set once during local Mesh assembly.
func (r *Runner) SetRetainedArtifacts(retains bool) { r.retainsArtifacts = retains }

func (r *Runner) SetTakeoverArtifacts(stages bool) { r.takeoverArtifacts = stages }

func (s *state) recordModelIntent(step uint32, balance limits) bool {
	if s.checkpoint == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.runningLocked() {
		return false
	}
	d := &s.checkpoint.data
	d.Step, d.ModelInFlight, d.ModelAdmitted, d.Balance = step, true, false, balance
	d.ReconcileUntil = time.Time{}
	return s.saveLocked()
}

func (s *state) recordToolIntent(e mesh.EnvironmentRequest, req contract.ToolRequest, balance limits) bool {
	if s.checkpoint == nil {
		return true
	}
	d := &s.checkpoint.data
	d.Intent = &toolIntent{Key: contract.ReceiptKey{
		Runner: e.Run.Runner, Subject: e.Run.Subject, RunID: req.RunID, Step: s.step, Remote: d.RemoteBindings[req.Tool],
		CallID: req.CallID, Tool: req.Tool, BindingVersion: d.BindingVersion, AdmissionEpoch: e.Run.Epoch,
		Environment: e.Ref, Workspace: e.Run.Workspace, WorkspaceVersion: d.WorkspaceVersion, Fingerprint: durable.ToolFingerprint(req, e.Call.Path),
	}, Limits: req.Limits, Path: e.Call.Path}
	d.Balance, d.ReconcileUntil = balance, time.Time{}
	return s.saveLocked()
}

func (r *Runner) resumePosition(s *state, a *activity, balance *limits) bool {
	var verified contract.ToolRef
	if intent := s.checkpoint.data.Intent; intent != nil {
		result, ok := r.reconcileTool(s, a, balance, *intent)
		if !ok {
			return false
		}
		if result.State == contract.ToolExecutionKnown {
			verified = intent.Key.Tool
		}
	}
	s.mu.Lock()
	if !s.runningLocked() {
		if s.stop != nil && s.result.Outcome == "" && s.activeCall == "" && s.checkpointErr == nil {
			s.finishLocked(s.stopOutcome, nil, s.usage, s.stop)
		}
		s.mu.Unlock()
		return false
	}
	if s.checkpoint.data.ModelInFlight {
		s.checkpoint.data.Balance.Output = 0
		s.finishLocked(contract.Failed, nil, s.usage, &contract.StopDetail{Code: contract.StopModelInterrupted, Step: s.step})
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()

	if !r.resumeRemoteWorkspaces(s, a, verified) {
		return false
	}
	if a.env.Ref != "" && !r.awaitWorkspace(s, a) {
		s.failModel(contract.Failed, contract.StopWorkspaceUnavailable, 0)
		return false
	}
	return true
}

const reconcileInterval = 20 * time.Millisecond
const reconcileQueryTimeout = 100 * time.Millisecond

func (s *state) reconciliationDeadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.runningLocked() {
		return time.Time{}
	}
	d := &s.checkpoint.data
	if d.ReconcileUntil.IsZero() {
		d.ReconcileUntil = time.Now().Add(d.ReconcileTimeout)
		if d.ReconcileUntil.After(s.deadline) {
			d.ReconcileUntil = s.deadline
		}
		if !s.saveLocked() {
			return time.Time{}
		}
	}
	return d.ReconcileUntil
}

func (r *Runner) reconcileTool(s *state, a *activity, balance *limits, intent toolIntent) (contract.ToolResult, bool) {
	key := intent.Key
	req := contract.ToolRequest{RunID: key.RunID, CallID: key.CallID, Tool: key.Tool, Limits: intent.Limits}
	e := a.env
	e.Call = contract.CallScope{Run: e.Run, CallID: key.CallID, Tool: key.Tool, Limits: intent.Limits}
	var result contract.ToolResult
	var verified contract.ToolReceipt
	until := s.reconciliationDeadline()
	if s.reconcile(until, func(ctx context.Context) bool {
		reply, err := mesh.CallReceipt(ctx, a.transport, mesh.QueryReceipt, e, req, key)
		if err != nil || reply.Receipt == nil || reply.Receipt.Key != key {
			return false
		}
		value := reply.Receipt.Result
		if !contract.ValidToolResult(value, key.RunID, key.CallID) || toolStatus(value) == contract.ToolStatusUnknown {
			return false
		}
		if ref := reply.Receipt.Evidence; ref != nil {
			if key.Remote.EvidenceRule != contract.AppendFileEvidenceV1 || ref.ID != durable.WriteEvidenceID(key) ||
				!contract.ValidContentHash(ref.Version) || toolStatus(value) != contract.ToolStatusSucceeded {
				return false
			}
			var file contract.AppendFileResult
			if json.Unmarshal([]byte(value.Completion.Content), &file) != nil || file.Path != intent.Path ||
				file.Version != ref.Version || file.SHA256 != ref.Version || file.Bytes == 0 {
				return false
			}
		}
		if value.Usage != nil {
			scanned, raw := valueOr(value.Usage.ScannedBytes, 0), valueOr(value.Usage.RawOutputBytes, 0)
			if scanned > intent.Limits.MaxScanBytes || raw > intent.Limits.MaxRawOutputBytes ||
				(value.State == contract.ToolExecutionNotExecuted && (scanned != 0 || raw != 0)) {
				return false
			}
		}
		if _, fits := receiptSize(value, intent.Limits.MaxResultBytes); !fits {
			return false
		}
		if value.Completion != nil {
			for _, ref := range value.Completion.Artifacts {
				if intent.Limits.MaxRawOutputBytes == 0 || ref.SizeBytes > intent.Limits.MaxRawOutputBytes ||
					mesh.CheckRetainedArtifact(ctx, a.transport, e, ref) != nil {
					return false
				}
			}
		}
		verified = *reply.Receipt
		verified.Result = cloneToolResult(value)
		return true
	}, key.Remote != (contract.RemoteToolIdentity{})) {
		result = verified.Result
	}
	scanned, raw := intent.Limits.MaxScanBytes, intent.Limits.MaxRawOutputBytes
	if result.State == contract.ToolExecutionNotExecuted {
		scanned, raw = 0, 0
	}
	// Recovery cannot re-read the original environment and artifact meters.
	if key.Remote != (contract.RemoteToolIdentity{}) {
		s.recordRemoteCleanup(verified.Cleanup)
	}
	settled := s.settleTool(balance, req, result, toolStatus(result), true, scanned, true, raw, verified.Evidence)
	if settled && key.Remote != (contract.RemoteToolIdentity{}) && result.State != contract.ToolExecutionNotExecuted {
		return result, r.ensureRemoteWorkspace(s, a, key.Tool, key.Remote)
	}
	return result, settled
}

func (r *Runner) awaitWorkspace(s *state, a *activity) bool {
	return s.reconcile(s.reconciliationDeadline(), func(ctx context.Context) bool {
		version, err := r.workspaceVersion(ctx, a)
		return err == nil && version == s.checkpoint.data.WorkspaceVersion
	}, false)
}

func (s *state) recordRemoteCleanup(cleanup contract.CleanupStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cleanup != contract.CleanupReleased {
		cleanup = contract.CleanupPending
	}
	if s.checkpoint.data.RemoteCleanup != contract.CleanupPending {
		s.checkpoint.data.RemoteCleanup = cleanup
	}
}

func (r *Runner) resumeRemoteWorkspaces(s *state, a *activity, verified contract.ToolRef) bool {
	c := &s.checkpoint.data
	if len(c.RemoteBindings) == 0 {
		return true
	}
	needed := make(map[contract.ToolRef]bool)
	for _, message := range s.messages {
		if m := message.Tool; m != nil && m.Result.State == contract.ToolExecutionKnown {
			needed[m.Tool] = true
		}
	}
	for i, result := range c.ToolResults {
		if result.State == contract.ToolExecutionKnown {
			needed[c.Pending.ToolCalls[i].Tool] = true
		}
	}
	for _, tool := range s.request.Tools {
		if needed[tool] && tool != verified && !r.ensureRemoteWorkspace(s, a, tool, c.RemoteBindings[tool]) {
			return false
		}
	}
	return true
}

func (r *Runner) ensureRemoteWorkspace(s *state, a *activity, tool contract.ToolRef, remote contract.RemoteToolIdentity) bool {
	if remote.Workspace == "" {
		return true
	}
	key := contract.ReceiptKey{Remote: remote, Tool: tool, Subject: a.env.Run.Subject, Runner: a.env.Run.Runner,
		RunID: a.env.Run.RunID, BindingVersion: s.checkpoint.data.BindingVersion}
	if s.reconcile(s.reconciliationDeadline(), func(ctx context.Context) bool {
		_, err := mesh.CallReceipt(ctx, a.transport, mesh.VerifyRemoteToolWorkspace, a.env, contract.ToolRequest{}, key)
		return err == nil
	}, true) {
		return true
	}
	s.failModel(contract.Failed, contract.StopWorkspaceUnavailable, 0)
	return false
}

// Only one query can be in flight even if a local provider disregards cancellation.
func (s *state) reconcile(until time.Time, query func(context.Context) bool, remote bool) bool {
	if !time.Now().Before(until) || s.ctx.Err() != nil {
		return false
	}
	window, cancel := context.WithDeadline(s.ctx, until)
	defer cancel()
	delay, queryTimeout := reconcileInterval, reconcileQueryTimeout
	if remote {
		queryTimeout = 250 * time.Millisecond
	}
	for window.Err() == nil {
		ctx, cancelQuery := context.WithTimeout(window, queryTimeout)
		result := make(chan bool, 1)
		go func() { result <- query(ctx) }()
		select {
		case found := <-result:
			valid := ctx.Err() == nil && time.Now().Before(until)
			cancelQuery()
			if found && valid {
				return true
			}
		case <-ctx.Done():
			cancelQuery()
			// Wait out a stuck query without starting another one.
			select {
			case <-window.Done():
				return false
			case <-result:
			}
		}
		timer := time.NewTimer(delay)
		if remote {
			delay = min(delay*2, 100*time.Millisecond)
		}
		select {
		case <-window.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return false
}
