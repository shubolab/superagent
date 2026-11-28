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
	Execute   func(context.Context, contract.ToolRequest) (contract.ToolResult, contract.CleanupStatus, error)
}

type receiptRecord struct {
	Key     contract.ReceiptKey
	Write   *contract.WriteIntent `json:",omitempty"`
	Result  *contract.ToolResult
	Cleanup contract.CleanupStatus `json:",omitempty"`
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
	key, recoverable := contract.InvocationReceiptKey(ctx)
	if !recoverable {
		return t.Tool.Invoke(ctx, req)
	}
	receipt, err := t.InvokeReceipt(ctx, key, req)
	if err != nil {
		return contract.ToolResult{}, err
	}
	return receipt.Result, nil
}

// InvokeReceipt executes and records one result before returning it to the transport.
func (t *ReceiptTool) InvokeReceipt(ctx context.Context, key contract.ReceiptKey, req contract.ToolRequest) (contract.ToolReceipt, error) {
	return t.invokeReceipt(ctx, key, req, nil)
}

func (t *ReceiptTool) InvokeWriteReceipt(ctx context.Context, key contract.ReceiptKey, req contract.ToolRequest, intent contract.WriteIntent) (contract.ToolReceipt, error) {
	if intent.Key != key || intent.Limits != req.Limits {
		return contract.ToolReceipt{}, contract.ErrInvalidRequest
	}
	return t.invokeReceipt(ctx, key, req, &intent)
}

func (t *ReceiptTool) invokeReceipt(ctx context.Context, key contract.ReceiptKey, req contract.ToolRequest, intent *contract.WriteIntent) (contract.ToolReceipt, error) {
	empty := contract.ToolReceipt{}
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
		return contract.ToolReceipt{Key: key, Result: *record.Result, Cleanup: record.Cleanup}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	record.Key, record.Write = key, intent
	if _, err := Write(path, record); err != nil {
		return empty, err
	}
	var result contract.ToolResult
	var cleanup contract.CleanupStatus
	if t.Execute != nil {
		result, cleanup, err = t.Execute(ctx, req)
	} else {
		result, err = t.Tool.Invoke(ctx, req)
	}
	if err != nil {
		return empty, err
	}
	reply := contract.ToolReceipt{Key: key, Result: result, Cleanup: cleanup}
	if !contract.ValidToolResult(result, key.RunID, key.CallID) ||
		(result.State != contract.ToolExecutionKnown && result.State != contract.ToolExecutionNotExecuted) ||
		(result.Completion != nil && (len(result.Completion.Artifacts) > 1 ||
			uint64(len(result.Completion.Content)) > min(req.Limits.MaxResultBytes, MaxRecordBytes))) {
		return reply, nil
	}
	encoded, err := json.Marshal(result)
	if err != nil || uint64(len(encoded)) > req.Limits.MaxResultBytes {
		return reply, nil
	}
	// Filesystem commits are independent of cancellation of the response connection.
	record.Result, record.Cleanup = &result, cleanup
	if _, err := Write(path, record); err != nil {
		return empty, err
	}
	return reply, nil
}

func (t *ReceiptTool) QueryReceipt(ctx context.Context, key contract.ReceiptKey) (*contract.ToolReceipt, error) {
	result, _, err := t.LookupReceipt(ctx, key)
	return result, err
}

// LookupReceipt exposes the matching durable write admission only while its receipt is pending.
func (t *ReceiptTool) LookupReceipt(ctx context.Context, key contract.ReceiptKey) (*contract.ToolReceipt, *contract.WriteIntent, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var record receiptRecord
	if err := Read(t.path(key), &record); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if record.Key != key {
		return nil, nil, contract.ErrAccessDenied
	}
	if record.Result == nil {
		if record.Write != nil && record.Write.Key != key {
			return nil, nil, contract.ErrAccessDenied
		}
		return nil, record.Write, nil
	}
	return &contract.ToolReceipt{Key: record.Key, Result: *record.Result, Cleanup: record.Cleanup}, nil, nil
}

func ToolFingerprint(req contract.ToolRequest, path string) string {
	digest, _ := Digest(struct {
		Tool      contract.ToolRef
		Arguments json.RawMessage
		Path      string
		Limits    contract.ToolLimits
	}{req.Tool, req.Arguments, path, req.Limits})
	return digest
}

func WriteEvidenceID(key contract.ReceiptKey) string {
	id, _ := Digest([4]string{string(key.Runner), string(key.Subject), string(key.RunID), string(key.CallID)})
	return id
}
