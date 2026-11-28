package sdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/mesh"
	"github.com/shubolab/superagent/schedule"
	api "github.com/shubolab/superagent/session"
)

func (s *Session) startScheduled(ctx context.Context, runner mesh.SessionRunner, req contract.StartRequest, digest string, report func(schedule.AdmissionPhase)) (*Run, error) {
	if !s.owns(req.ID) {
		return nil, ErrSessionOwnership
	}
	if digest == "" {
		var err error
		req, digest, err = freezeScheduledRequest(req)
		if err != nil {
			return nil, err
		}
	}
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return nil, err
	}
	record, err := s.store.LookupCommit(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if record.Receipt != nil {
		return nil, contract.ErrStartConflict
	}
	if idx.Pending != nil {
		if idx.Pending.Request.ID != req.ID {
			return nil, ErrSessionBusy
		}
		pending, err := awaiting(idx, req.ID)
		if err != nil {
			return nil, err
		}
		if _, err := s.frozenRequest(ctx, runner, pending, req); err != nil {
			return nil, err
		}
		// Retained terminal results are read without claiming execution capacity.
		if checkpoint, _ := runner.SessionCheckpoint(s.scope.Subject, req.ID, pending.RequestDigest); checkpoint.Result != nil {
			return s.start(ctx, runner, req)
		}
	}
	return s.admitScheduled(ctx, runner, req.ID, digest, report, func(ctx context.Context) (*Run, error) {
		return s.start(ctx, runner, req)
	})
}

func (s *Session) recoverScheduled(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (*Run, error) {
	pending, checkpoint, err := s.scheduledCheckpoint(ctx, runner, id)
	if err != nil {
		return nil, err
	}
	if checkpoint.Result != nil {
		return s.recoverRun(ctx, runner, id)
	}
	return s.admitRecovery(ctx, runner, pending)
}

func (s *Session) scheduledCheckpoint(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (api.Reservation, contract.SessionRunCheckpoint, error) {
	var empty contract.SessionRunCheckpoint
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return api.Reservation{}, empty, err
	}
	pending, err := awaiting(idx, id)
	if err != nil {
		return pending, empty, err
	}
	checkpoint, err := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if err != nil {
		return pending, empty, sessionResultError(err)
	}
	return pending, checkpoint, nil
}

func (s *Session) admitRecovery(ctx context.Context, runner mesh.SessionRunner, pending api.Reservation) (*Run, error) {
	_, digest, err := freezeScheduledRequest(pending.Request)
	if err != nil {
		return nil, err
	}
	id := pending.Request.ID
	return s.admitScheduled(ctx, runner, id, digest, nil, func(ctx context.Context) (*Run, error) {
		return s.recoverRun(ctx, runner, id)
	})
}

func (s *Session) admitScheduled(ctx context.Context, runner mesh.SessionRunner, id contract.RunID, digest string, report func(schedule.AdmissionPhase), work func(context.Context) (*Run, error)) (*Run, error) {
	owner := schedule.Session{Subject: s.scope.Subject, ID: s.scope.Session}
	err := runner.SessionScheduler().AdmitWithProgress(ctx, owner, id, digest, report, func(ctx context.Context) error {
		_, err := work(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.client.Run(id), nil
}

// Freeze only caller-supplied parameters. History is loaded after the permit is granted.
func freezeScheduledRequest(req contract.StartRequest) (contract.StartRequest, string, error) {
	req.History, req.PinnedHistory = nil, nil
	size := len(req.Input.Text) + len(req.Instruction) + len(req.Model) + len(req.ID)
	if req.Environment != nil {
		size += len(req.Environment.Ref) + len(req.Environment.Workspace)
	}
	if len(req.Tools) > durable.MaxRecordBytes/3 {
		return contract.StartRequest{}, "", ErrSessionCapacity
	}
	for _, ref := range req.Tools {
		size += len(ref)
		if size > durable.MaxRecordBytes {
			return contract.StartRequest{}, "", ErrSessionCapacity
		}
	}
	if size > durable.MaxRecordBytes {
		return contract.StartRequest{}, "", ErrSessionCapacity
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return contract.StartRequest{}, "", err
	}
	if len(encoded) > durable.MaxRecordBytes {
		return contract.StartRequest{}, "", ErrSessionCapacity
	}
	var frozen contract.StartRequest
	if err := json.Unmarshal(encoded, &frozen); err != nil {
		return contract.StartRequest{}, "", err
	}
	sum := sha256.Sum256(encoded)
	return frozen, hex.EncodeToString(sum[:]), nil
}
