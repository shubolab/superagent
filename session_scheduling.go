package superagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/schedule"
	"github.com/shubolab/superagent/internal/session"
)

func (s *Session) startScheduled(ctx context.Context, runner mesh.SessionRunner, req StartRequest) (*Run, error) {
	if !s.store.Owns(req.ID) {
		return nil, ErrSessionOwnership
	}
	req, digest, err := freezeScheduledRequest(req)
	if err != nil {
		return nil, err
	}
	idx, err := s.store.Load()
	if err != nil {
		return nil, err
	}
	if _, committed := idx.Receipt(req.ID); committed {
		return nil, ErrStartConflict
	}
	if idx.Pending != nil {
		if idx.Pending.Request.ID != req.ID {
			return nil, ErrSessionBusy
		}
		pending, err := idx.Awaiting(req.ID)
		if err != nil {
			return nil, err
		}
		if _, err := s.frozenRequest(runner, idx, pending, req); err != nil {
			return nil, err
		}
		// Retained terminal results are read without claiming execution capacity.
		if checkpoint, _ := runner.SessionCheckpoint(s.scope.Subject, req.ID, pending.RequestDigest); checkpoint.Result != nil {
			return s.start(ctx, runner, req)
		}
	}
	return s.admitScheduled(ctx, runner, req.ID, digest, func(ctx context.Context) (*Run, error) {
		return s.start(ctx, runner, req)
	})
}

func (s *Session) recoverScheduled(ctx context.Context, runner mesh.SessionRunner, id RunID) (*Run, error) {
	_, pending, checkpoint, err := s.scheduledCheckpoint(runner, id)
	if err != nil {
		return nil, err
	}
	if checkpoint.Result != nil {
		return s.recoverRun(ctx, runner, id)
	}
	return s.admitRecovery(ctx, runner, pending)
}

func (s *Session) scheduledCheckpoint(runner mesh.SessionRunner, id RunID) (session.Index, session.Pending, contract.SessionRunCheckpoint, error) {
	var empty contract.SessionRunCheckpoint
	idx, err := s.store.Load()
	if err != nil {
		return idx, session.Pending{}, empty, err
	}
	pending, err := idx.Awaiting(id)
	if err != nil {
		return idx, pending, empty, err
	}
	checkpoint, err := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if err != nil {
		return idx, pending, empty, sessionResultError(err)
	}
	return idx, pending, checkpoint, nil
}

func (s *Session) admitRecovery(ctx context.Context, runner mesh.SessionRunner, pending session.Pending) (*Run, error) {
	_, digest, err := freezeScheduledRequest(pending.Request)
	if err != nil {
		return nil, err
	}
	id := pending.Request.ID
	return s.admitScheduled(ctx, runner, id, digest, func(ctx context.Context) (*Run, error) {
		return s.recoverRun(ctx, runner, id)
	})
}

func (s *Session) admitScheduled(ctx context.Context, runner mesh.SessionRunner, id RunID, digest string, work func(context.Context) (*Run, error)) (*Run, error) {
	owner := schedule.Session{Subject: s.scope.Subject, ID: s.scope.Session}
	err := runner.SessionScheduler().Admit(ctx, owner, id, digest, func(ctx context.Context) error {
		_, err := work(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.client.Run(id), nil
}

// Freeze only caller-supplied parameters. History is loaded after the permit is granted.
func freezeScheduledRequest(req StartRequest) (StartRequest, string, error) {
	req.History, req.PinnedHistory = nil, nil
	size := len(req.Input.Text) + len(req.Instruction) + len(req.Model) + len(req.ID)
	if req.Environment != nil {
		size += len(req.Environment.Ref) + len(req.Environment.Workspace)
	}
	if len(req.Tools) > durable.MaxRecordBytes/3 {
		return StartRequest{}, "", ErrSessionCapacity
	}
	for _, ref := range req.Tools {
		size += len(ref)
		if size > durable.MaxRecordBytes {
			return StartRequest{}, "", ErrSessionCapacity
		}
	}
	if size > durable.MaxRecordBytes {
		return StartRequest{}, "", ErrSessionCapacity
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return StartRequest{}, "", err
	}
	if len(encoded) > durable.MaxRecordBytes {
		return StartRequest{}, "", ErrSessionCapacity
	}
	var frozen StartRequest
	if err := json.Unmarshal(encoded, &frozen); err != nil {
		return StartRequest{}, "", err
	}
	sum := sha256.Sum256(encoded)
	return frozen, hex.EncodeToString(sum[:]), nil
}
