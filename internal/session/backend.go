package session

import (
	"context"
	"errors"
	"os"

	"github.com/shubolab/superagent/internal/contract"
	api "github.com/shubolab/superagent/session"
)

type Files struct{ Directory string }

func (f Files) Open(ctx context.Context, scope Scope, limits api.Limits) (api.Storage, error) {
	store, err := Open(ctx, Config{Directory: f.Directory, MaxTurns: limits.MaxTurns, MaxHistoryBytes: limits.MaxHistoryBytes}, scope)
	if err != nil {
		return nil, err
	}
	return Backend{Store: store}, nil
}

type Backend struct{ *Store }

func (b Backend) LoadState(ctx context.Context) (api.State, error) {
	if err := ctx.Err(); err != nil {
		return api.State{}, err
	}
	idx, err := b.Load()
	return api.State{Version: idx.Version, Pending: idx.Pending}, err
}

func (b Backend) ReadTurn(ctx context.Context, version uint64) (api.Turn, error) {
	if err := ctx.Err(); err != nil {
		return api.Turn{}, err
	}
	idx, err := b.Load()
	if err != nil {
		return api.Turn{}, err
	}
	turn, err := b.Store.ReadTurn(idx, version)
	return publicTurn(turn), err
}

func (b Backend) ReadWindow(ctx context.Context, version, first, limit uint64, exact bool) (uint64, []contract.Message, error) {
	idx, err := b.Load()
	if err != nil {
		return 0, nil, err
	}
	if idx.Version < version {
		return 0, nil, ErrVersion
	}
	return ReadWindow(version, first, limit, exact, func(at uint64) ([]contract.Message, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		turn, err := b.Store.ReadTurn(idx, at)
		return turn.Messages, err
	})
}

func publicTurn(turn Turn) api.Turn {
	return api.Turn{Receipt: api.Receipt(turn.Receipt), RequestDigest: turn.RequestDigest, Outcome: turn.Outcome, Messages: turn.Messages}
}

func (b Backend) WithAdmission(ctx context.Context, id contract.RunID, work func(context.Context) error) error {
	lock, err := b.AdmissionLock(ctx, id)
	if err != nil {
		return err
	}
	defer lock.Close()
	return work(ctx)
}

func (b Backend) lockedIndex(ctx context.Context, work func(Index) error) error {
	lock, err := b.Lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	idx, err := b.Load()
	if err != nil {
		return err
	}
	return work(idx)
}

func (b Backend) Reserve(ctx context.Context, pending api.Reservation) error {
	return b.lockedIndex(ctx, func(idx Index) error {
		if _, exists := idx.Receipt(pending.Request.ID); exists {
			return contract.ErrStartConflict
		}
		if idx.Pending != nil {
			return ErrBusy
		}
		if pending.Version != idx.Version {
			return ErrVersion
		}
		if !b.Owns(pending.Request.ID) {
			return ErrOwnership
		}
		if err := b.CheckCapacity(idx, pending); err != nil {
			return err
		}
		idx.Pending = &pending
		return b.Save(idx)
	})
}

func (b Backend) MarkAttempted(ctx context.Context, key api.ReservationKey) error {
	return b.changeReservation(ctx, key, true)
}

func (b Backend) ClearReservation(ctx context.Context, key api.ReservationKey) error {
	return b.changeReservation(ctx, key, false)
}

func (b Backend) changeReservation(ctx context.Context, key api.ReservationKey, attempt bool) error {
	return b.lockedIndex(ctx, func(idx Index) error {
		pending, err := idx.Awaiting(key.RunID)
		if err != nil {
			return err
		}
		if pending.Key() != key || (attempt && key.Attempted) {
			return contract.ErrStartConflict
		}
		if attempt {
			idx.Pending.Attempted = true
		} else {
			if _, err := b.ReadSealed(idx, key.RunID); !errors.Is(err, os.ErrNotExist) {
				return errors.Join(ErrContent, err)
			}
			idx.Pending = nil
		}
		return b.Save(idx)
	})
}

func (b Backend) LookupCommit(ctx context.Context, id contract.RunID) (record api.CommitRecord, err error) {
	err = b.lockedIndex(ctx, func(idx Index) error {
		if receipt, ok := idx.Receipt(id); ok {
			value := api.Receipt(receipt)
			record.Receipt = &value
			return nil
		}
		if idx.Pending == nil || idx.Pending.Request.ID != id {
			return nil
		}
		turn, err := b.ReadSealed(idx, id)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		value := publicTurn(turn)
		record.Sealed = &value
		return nil
	})
	return record, err
}

func (b Backend) Seal(ctx context.Context, turn api.Turn) error {
	return b.lockedIndex(ctx, func(idx Index) error {
		if receipt, ok := idx.Receipt(turn.RunID); ok {
			if api.Receipt(receipt) != turn.Receipt {
				return ErrContent
			}
			return b.Sync()
		}
		pending, err := idx.Awaiting(turn.RunID)
		if err != nil {
			return err
		}
		if turn.Version != idx.Version+1 {
			return ErrVersion
		}
		digest, err := resultDigest(turn.Outcome, turn.Messages)
		if err != nil || digest != turn.Digest || pending.RequestDigest != turn.RequestDigest {
			return ErrContent
		}
		_, _, err = b.PrepareCommit(idx, turn.RunID, &contract.Result{RunID: turn.RunID, Outcome: turn.Outcome, Messages: turn.Messages})
		return err
	})
}

func (b Backend) Publish(ctx context.Context, id contract.RunID, digest string) (published api.Receipt, err error) {
	err = b.lockedIndex(ctx, func(idx Index) error {
		if receipt, ok := idx.Receipt(id); ok {
			if receipt.Digest != digest {
				return ErrContent
			}
			published = api.Receipt(receipt)
			return b.Sync()
		}
		turn, err := b.ReadSealed(idx, id)
		if err != nil {
			return err
		}
		if turn.Digest != digest {
			return ErrContent
		}
		receipt, err := b.Store.Publish(idx, turn)
		published = api.Receipt(receipt)
		return err
	})
	return published, err
}
