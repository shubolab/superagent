package remotetool

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func (s *server) queryReceipt(ctx context.Context, msg request) (*contract.ToolReceipt, error) {
	receipt, intent, err := s.journal.LookupReceipt(ctx, msg.Key)
	if err != nil || receipt != nil || intent == nil || s.cfg.Identity.EvidenceRule != contract.AppendFileEvidenceV1 {
		return receipt, err
	}
	if intent.Limits != msg.Limits || !time.Now().Before(intent.RetainUntil) {
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	env := s.cfg.Environment.(contract.EvidenceWorkspace)
	queryCtx, end := contract.WithWriteEvidenceQuery(ctx, *intent)
	defer end()
	evidence, err := env.ReadWriteEvidence(queryCtx, *intent)
	if err != nil {
		return nil, err
	}
	result, err := renderAppendResult(*intent, evidence)
	if err != nil {
		return nil, err
	}
	// The write proof does not say that the original resources were released.
	return &contract.ToolReceipt{Key: msg.Key, Result: result, Evidence: &evidence.Ref, Cleanup: contract.CleanupPending}, nil
}

func (s *server) executeWrite(ctx context.Context, call execution, req contract.ToolRequest) (result contract.ToolResult, cleanup contract.CleanupStatus, err error) {
	if err := s.verifyWorkspace(ctx, call.Scope); err != nil {
		return result, cleanup, err
	}
	env := s.cfg.Environment
	scope := call.Scope
	scope.Workspace = call.Key.Remote.Workspace
	handle, err := env.Allocate(ctx, scope)
	if err != nil {
		return result, contract.CleanupPending, err
	}
	cs := contract.CallScope{Run: scope, CallID: req.CallID, Tool: req.Tool, Handle: handle, Path: call.path, Limits: req.Limits}
	var grant contract.EnvironmentGrantRef
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		var revokeErr error
		if grant != "" {
			_, revokeErr = env.Revoke(cleanupCtx, cs, grant)
		}
		cleanup = contract.CleanupReleased
		if errors.Join(revokeErr, env.Release(cleanupCtx, scope, handle)) != nil {
			cleanup = contract.CleanupPending
		}
	}()
	grant, err = env.Grant(ctx, cs, ".")
	if err != nil {
		return result, cleanup, err
	}
	workspace := env.(contract.EvidenceWorkspace)
	intent := *call.write
	writeCtx, endStage := contract.WithWorkspaceWriteAccess(ctx, cs, grant, intent, nil)
	staged, err := workspace.StageWrite(writeCtx, cs, grant, intent, call.data)
	endStage()
	if err != nil {
		return result, cleanup, err
	}
	prepared := contract.WriteEvidence{Intent: intent, BaseVersion: staged.BaseVersion, Version: staged.Version,
		Ref: contract.WriteEvidenceRef{ID: durable.WriteEvidenceID(intent.Key), Version: staged.Version.ID}}
	// Reject a recipe that cannot produce a usable result before publishing.
	result, err = renderAppendResult(intent, prepared)
	if err != nil {
		return result, cleanup, err
	}
	publish := func() error {
		commitCtx, endCommit := contract.WithWorkspaceWriteAccess(ctx, cs, grant, intent, &staged)
		defer endCommit()
		evidence, err := workspace.CommitWrite(commitCtx, cs, grant, intent, staged)
		if err != nil {
			return err
		}
		if !evidence.Intent.Matches(intent) || evidence.Ref != prepared.Ref || evidence.Version != staged.Version || evidence.BaseVersion != staged.BaseVersion {
			return contract.ErrWriteEvidenceUnavailable
		}
		return nil
	}
	if call.Owner.Epoch != 0 {
		err = s.cfg.Authority.PublishRemote(ctx, call.Key, call.Scope, call.Owner, publish)
	} else {
		err = publish()
	}
	if err != nil {
		return contract.ToolResult{}, cleanup, err
	}
	return result, cleanup, nil
}

func renderAppendResult(intent contract.WriteIntent, evidence contract.WriteEvidence) (contract.ToolResult, error) {
	if intent.Key.Remote.EvidenceRule != contract.AppendFileEvidenceV1 || !evidence.Intent.Matches(intent) ||
		!contract.ValidManagedPath(intent.Path) || !contract.ValidContentHash(intent.AppendHash) ||
		intent.AppendBytes == 0 || intent.AppendBytes > contract.MaxWorkspacePublishBytes || intent.AppendBytes > evidence.Version.Bytes ||
		evidence.Ref.ID != durable.WriteEvidenceID(intent.Key) || evidence.Ref.Version != evidence.Version.ID ||
		!contract.ValidContentHash(evidence.Version.ID) || evidence.Version.Bytes == 0 || evidence.Version.Bytes > durable.MaxRecordBytes ||
		(evidence.BaseVersion != "" && !contract.ValidContentHash(evidence.BaseVersion)) {
		return contract.ToolResult{}, contract.ErrWriteEvidenceUnavailable
	}
	content, _ := json.Marshal(contract.AppendFileResult{
		Path: intent.Path, Version: evidence.Version.ID, Bytes: evidence.Version.Bytes, SHA256: evidence.Version.ID,
	})
	result := contract.ToolResult{RunID: intent.Key.RunID, CallID: intent.Key.CallID, State: contract.ToolExecutionKnown,
		Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: string(content)}}
	if !validReceipt(&contract.ToolReceipt{Key: intent.Key, Result: result}, intent.Key, intent.Limits.MaxResultBytes) {
		return contract.ToolResult{}, contract.ErrWriteEvidenceUnavailable
	}
	return result, nil
}
