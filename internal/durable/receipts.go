package durable

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/shubolab/superagent/internal/contract"
)

type ReceiptTool struct {
	Directory string
	Tool      contract.Tool
}

type receiptRecord struct {
	Key    contract.ReceiptKey
	Result *contract.ToolResult
}

func (t *ReceiptTool) path(key contract.ReceiptKey) string {
	// A changed fingerprint or binding still collides with the original execution.
	id := struct {
		Runner  contract.RunnerRef
		Subject contract.SubjectID
		RunID   contract.RunID
		CallID  contract.CallID
	}{key.Runner, key.Subject, key.RunID, key.CallID}
	digest, _ := Digest(id)
	return filepath.Join(t.Directory, digest+".receipt")
}

func (t *ReceiptTool) Invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	empty := contract.ToolResult{}
	key, recoverable := contract.InvocationReceiptKey(ctx)
	if !recoverable {
		return t.Tool.Invoke(ctx, req)
	}
	if key.RunID != req.RunID || key.CallID != req.CallID || key.Tool != req.Tool {
		return empty, contract.ErrAccessDenied
	}
	path := t.path(key)
	lock, err := Lock(path)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	var record receiptRecord
	err = Read(path, &record)
	if err == nil {
		if record.Key != key {
			return empty, contract.ErrStartConflict
		}
		if record.Result == nil {
			return empty, contract.ErrRecoveryUnavailable
		}
		return *record.Result, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	record.Key = key
	if _, err := Write(path, record); err != nil {
		return empty, err
	}
	result, err := t.Tool.Invoke(ctx, req)
	if err != nil {
		return empty, err
	}
	if result.RunID != key.RunID || result.CallID != key.CallID ||
		(result.State != contract.ToolExecutionKnown && result.State != contract.ToolExecutionNotExecuted) ||
		(result.Completion != nil && (len(result.Completion.Artifacts) > 1 ||
			uint64(len(result.Completion.Content)) > min(req.Limits.MaxResultBytes, MaxRecordBytes))) {
		// An unknown result leaves the durable pending record intact.
		return result, nil
	}
	encoded, err := json.Marshal(result)
	if err != nil || uint64(len(encoded)) > req.Limits.MaxResultBytes {
		return result, nil
	}
	record.Result = &result
	if _, err := Write(path, record); err != nil {
		return empty, err
	}
	return result, nil
}

func (t *ReceiptTool) QueryReceipt(ctx context.Context, key contract.ReceiptKey) (*contract.ToolReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var record receiptRecord
	if err := Read(t.path(key), &record); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if record.Key != key {
		return nil, contract.ErrAccessDenied
	}
	if record.Result == nil {
		return nil, nil
	}
	return &contract.ToolReceipt{Key: record.Key, Result: *record.Result}, nil
}
