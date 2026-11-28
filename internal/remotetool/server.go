package remotetool

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type Access struct {
	Invoke      bool
	ReadReceipt bool
}

type Authority interface {
	AdmitRemote(context.Context, contract.ReceiptKey, contract.RunScope, contract.RemoteOwner, contract.ToolRequest, string) error
	ReadRemote(context.Context, contract.ReceiptKey, contract.RunScope, contract.RemoteOwner, bool) error
	PublishRemote(context.Context, contract.ReceiptKey, contract.RunScope, contract.RemoteOwner, func() error) error
}

type ServerConfig struct {
	Identity          contract.RemoteToolIdentity
	ToolRef           contract.ToolRef
	Binding           contract.LocalToolBinding
	Environment       contract.EnvironmentProvider
	ReceiptDirectory  string
	Authorize         func(context.Context, *x509.Certificate, contract.ReceiptKey) Access
	Authority         Authority
	EvidenceRetention time.Duration
}

type server struct {
	cfg     ServerConfig
	journal *durable.ReceiptTool
}

func NewHandler(cfg ServerConfig) (http.Handler, error) {
	spec := cfg.Binding.Spec
	usesEvidence := cfg.Identity.EvidenceRule != ""
	if !validIdentity(cfg.Identity) || cfg.ToolRef == "" || (cfg.Binding.Tool == nil && !usesEvidence) || cfg.Authorize == nil ||
		spec.NeedsArtifactOutput || spec.NeedsContextSource || spec.Limits.MaxResultBytes == 0 || spec.Limits.MaxResultBytes > maxReceiptBytes {
		return nil, contract.ErrInvalidConfiguration
	}
	if cfg.Identity.Workspace != "" {
		if _, ok := cfg.Environment.(contract.RecoveryWorkspace); !ok {
			return nil, contract.ErrInvalidConfiguration
		}
	}
	if usesEvidence {
		if _, ok := cfg.Environment.(contract.EvidenceWorkspace); !ok || !spec.NeedsWorkspaceWrite ||
			spec.NeedsReadAccess || cfg.Binding.Tool != nil || cfg.EvidenceRetention <= 0 || cfg.Binding.Prepare != nil {
			return nil, contract.ErrInvalidConfiguration
		}
	}
	if spec.NeedsReadAccess || spec.NeedsWorkspaceWrite {
		if cfg.Identity.Workspace == "" || (cfg.Binding.Prepare == nil && !usesEvidence) || spec.Limits.MaxScanBytes == 0 {
			return nil, contract.ErrInvalidConfiguration
		}
		if spec.NeedsWorkspaceWrite {
			if _, ok := cfg.Environment.(contract.TakeoverWorkspace); !ok {
				return nil, contract.ErrInvalidConfiguration
			}
		}
	}
	if !durable.Supported {
		return nil, contract.ErrInvalidConfiguration
	}
	if err := durable.Directory(cfg.ReceiptDirectory); err != nil {
		return nil, err
	}
	s := &server{cfg: cfg}
	s.journal = &durable.ReceiptTool{Directory: cfg.ReceiptDirectory, Execute: s.execute}
	return s, nil
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	invoke := r.URL.Path == invokePath
	if r.Method != http.MethodPost || (!invoke && r.URL.Path != queryPath) {
		http.NotFound(w, r)
		return
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "authenticated client certificate required", http.StatusUnauthorized)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg request
	if readMessage(r.Body, maxRequestBytes, &msg) != nil || !s.validRequest(msg, invoke) {
		http.Error(w, "invalid tool request", http.StatusBadRequest)
		return
	}
	access := s.cfg.Authorize(r.Context(), r.TLS.PeerCertificates[0], msg.Key)
	if (invoke && !access.Invoke) || (!invoke && !access.ReadReceipt) {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}
	reply := response{ID: msg.ID, Identity: s.cfg.Identity}
	if invoke {
		receipt, err := s.invoke(r.Context(), msg)
		if errors.Is(err, contract.ErrStartConflict) {
			http.Error(w, "request fingerprint conflicts with original invocation", http.StatusConflict)
			return
		}
		if err != nil || !validReceipt(&receipt, msg.Key, msg.Limits.MaxResultBytes) {
			http.Error(w, "tool outcome unavailable", http.StatusServiceUnavailable)
			return
		}
		reply.Receipt = &receipt
	} else {
		until := minTime(msg.Deadline, msg.Scope.Deadline)
		ctx, cancel := context.WithDeadline(r.Context(), until)
		defer cancel()
		if msg.Owner.Epoch != 0 {
			if s.cfg.Authority == nil || s.cfg.Authority.ReadRemote(ctx, msg.Key, msg.Scope, msg.Owner, msg.Workspace) != nil {
				http.Error(w, "receipt access denied", http.StatusForbidden)
				return
			}
		}
		if msg.Workspace {
			reply.Ready = s.verifyWorkspace(ctx, msg.Scope) == nil
		} else {
			receipt, err := s.queryReceipt(ctx, msg)
			if err != nil || (receipt != nil && !validReceipt(receipt, msg.Key, msg.Limits.MaxResultBytes)) {
				http.Error(w, "receipt unavailable", http.StatusServiceUnavailable)
				return
			}
			reply.Receipt = receipt
		}
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}

func (s *server) validRequest(msg request, invoke bool) bool {
	key, scope := msg.Key, msg.Scope
	if msg.ID == "" || len(msg.ID) > 128 || key.Remote != s.cfg.Identity || key.Tool != s.cfg.ToolRef ||
		key.Subject == "" || key.Runner == "" || key.RunID == "" || key.BindingVersion == "" ||
		key.Subject != scope.Subject || key.Runner != scope.Runner || key.RunID != scope.RunID ||
		scope.Deadline.IsZero() || msg.Deadline.IsZero() || msg.Owner.Epoch != scope.Epoch ||
		(msg.Owner.Epoch == 0) != (msg.Owner.Instance == "") {
		return false
	}
	if msg.Workspace {
		return !invoke && msg.Tool == nil && key.CallID == "" && s.cfg.Identity.Workspace != ""
	}
	if key.CallID == "" || key.Step == 0 || key.Fingerprint == "" ||
		(key.AdmissionEpoch == 0) != (msg.Owner.Epoch == 0) || key.AdmissionEpoch > msg.Owner.Epoch ||
		key.Workspace != scope.Workspace ||
		msg.Limits.MaxResultBytes == 0 || msg.Limits.MaxResultBytes > s.cfg.Binding.Spec.Limits.MaxResultBytes ||
		msg.Limits.MaxScanBytes > s.cfg.Binding.Spec.Limits.MaxScanBytes || msg.Limits.MaxRawOutputBytes != 0 {
		return false
	}
	if !invoke {
		return msg.Tool == nil
	}
	req := msg.Tool
	return req != nil && key.AdmissionEpoch == scope.Epoch && req.RunID == key.RunID && req.CallID == key.CallID &&
		req.Tool == key.Tool && req.EnvironmentGrant == "" && req.Limits == msg.Limits && contract.JSONObject(req.Arguments)
}

type executionKey struct{}
type execution struct {
	request
	path  string
	write *contract.WriteIntent
	data  []byte
}

func (s *server) invoke(ctx context.Context, msg request) (contract.ToolReceipt, error) {
	call := execution{request: msg}
	if s.cfg.Identity.EvidenceRule == contract.AppendFileEvidenceV1 {
		path, data, err := contract.ParseEvidenceAppend(msg.Tool.Arguments)
		if err != nil {
			return contract.ToolReceipt{}, err
		}
		call.path, call.data = path, data
		call.write = &contract.WriteIntent{Key: msg.Key, Path: path, AppendHash: contract.HashBytes(data), AppendBytes: uint64(len(data)), Limits: msg.Limits,
			RetainUntil: msg.Scope.Deadline.Add(s.cfg.EvidenceRetention).UTC()}
	} else if s.cfg.Binding.Prepare != nil {
		var err error
		call.path, err = s.cfg.Binding.Prepare(msg.Tool.Arguments)
		if err != nil {
			return contract.ToolReceipt{}, err
		}
	}
	if durable.ToolFingerprint(*msg.Tool, call.path) != msg.Key.Fingerprint {
		return contract.ToolReceipt{}, contract.ErrInvalidRequest
	}
	// A disconnected response does not cancel the admitted operation or the journal commit.
	ctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), msg.Scope.Deadline)
	defer cancel()
	ctx = contract.WithReceiptKey(ctx, msg.Key)
	ctx = context.WithValue(ctx, executionKey{}, call)
	if call.write != nil {
		return s.journal.InvokeWriteReceipt(ctx, msg.Key, *msg.Tool, *call.write)
	}
	return s.journal.InvokeReceipt(ctx, msg.Key, *msg.Tool)
}

func (s *server) execute(ctx context.Context, req contract.ToolRequest) (result contract.ToolResult, cleanup contract.CleanupStatus, err error) {
	call := ctx.Value(executionKey{}).(execution)
	if call.Owner.Epoch != 0 {
		if s.cfg.Authority == nil {
			return result, cleanup, contract.ErrAccessDenied
		}
		if err := s.cfg.Authority.AdmitRemote(ctx, call.Key, call.Scope, call.Owner, req, call.path); err != nil {
			return result, cleanup, err
		}
	}
	if call.write != nil {
		return s.executeWrite(ctx, call, req)
	}
	spec := s.cfg.Binding.Spec
	if err := s.verifyWorkspace(ctx, call.Scope); err != nil {
		return result, cleanup, err
	}
	if !spec.NeedsReadAccess && !spec.NeedsWorkspaceWrite {
		result, err = s.cfg.Binding.Tool.Invoke(ctx, req)
		return result, contract.CleanupReleased, err
	}

	env := s.cfg.Environment
	scope := call.Scope
	scope.Workspace = s.cfg.Identity.Workspace
	handle, err := env.Allocate(ctx, scope)
	if err != nil {
		return result, contract.CleanupPending, err
	}
	callScope := contract.CallScope{Run: scope, CallID: req.CallID, Tool: req.Tool, Handle: handle, Path: call.path, Limits: req.Limits}
	var grant contract.EnvironmentGrantRef
	var published atomic.Bool
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		var revokeErr error
		var scanned uint64
		if grant != "" {
			scanned, revokeErr = env.Revoke(cleanupCtx, callScope, grant)
		}
		if result.State == contract.ToolExecutionNotExecuted && (published.Load() || scanned > 0 || revokeErr != nil) {
			err = contract.ErrRecoveryUnavailable
		}
		if err == nil && revokeErr == nil && scanned <= req.Limits.MaxScanBytes {
			result.Usage = &contract.ToolUsage{ScannedBytes: &scanned}
		}
		cleanup = contract.CleanupReleased
		if errors.Join(revokeErr, env.Release(cleanupCtx, scope, handle)) != nil {
			cleanup = contract.CleanupPending
		}
	}()
	grant, err = env.Grant(ctx, callScope, ".")
	if err != nil {
		return result, cleanup, err
	}
	req.EnvironmentGrant = grant
	authorized := func(claimed contract.ToolRequest, path string) bool {
		return claimed.RunID == req.RunID && claimed.CallID == req.CallID && claimed.Tool == req.Tool &&
			claimed.EnvironmentGrant == grant && claimed.Limits == req.Limits && path == call.path
	}
	toolCtx := ctx
	if spec.NeedsReadAccess {
		toolCtx = contract.WithFileOpener(toolCtx, func(openCtx context.Context, claimed contract.ToolRequest, path string) (io.ReadCloser, error) {
			if !authorized(claimed, path) || openCtx.Err() != nil {
				return nil, contract.ErrAccessDenied
			}
			return env.OpenRead(ctx, callScope, grant, path)
		})
	}
	if spec.NeedsWorkspaceWrite {
		workspace := env.(contract.TakeoverWorkspace)
		toolCtx = contract.WithWorkspacePublisher(toolCtx, func(writeCtx context.Context, claimed contract.ToolRequest, path string, data []byte) error {
			if !authorized(claimed, path) || writeCtx.Err() != nil || len(data) > contract.MaxWorkspacePublishBytes {
				return contract.ErrAccessDenied
			}
			publish := func() error {
				if err := s.verifyWorkspace(ctx, call.Scope); err != nil {
					return err
				}
				published.Store(true)
				return workspace.PublishWorkspace(contract.WithWorkspacePublication(ctx, callScope, grant), callScope, grant, data)
			}
			if call.Owner.Epoch != 0 {
				return s.cfg.Authority.PublishRemote(ctx, call.Key, call.Scope, call.Owner, publish)
			}
			return publish()
		})
	}
	result, err = s.cfg.Binding.Tool.Invoke(toolCtx, req)
	return result, cleanup, err
}

func (s *server) verifyWorkspace(ctx context.Context, scope contract.RunScope) error {
	if s.cfg.Identity.Workspace == "" {
		return nil
	}
	scope.Workspace = s.cfg.Identity.Workspace
	if scope.Epoch != 0 {
		fencer, ok := s.cfg.Environment.(contract.TakeoverWorkspace)
		if !ok || !fencer.FencesWorkspace(scope.Workspace) {
			return contract.ErrRecoveryUnavailable
		}
	}
	version, err := s.cfg.Environment.(contract.RecoveryWorkspace).RecoveryWorkspace(ctx, scope)
	if err != nil || version != s.cfg.Identity.WorkspaceVersion {
		return contract.ErrRecoveryUnavailable
	}
	return nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
