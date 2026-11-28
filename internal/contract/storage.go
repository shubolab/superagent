package contract

import (
	"context"
	"sync/atomic"
)

type storagePermit struct {
	ctx     context.Context
	revoked atomic.Bool
}

func (p *storagePermit) revoke() { p.revoked.Store(true) }

func (p *storagePermit) active() bool { return !p.revoked.Load() && p.ctx.Err() == nil }

// StorageLease orders a storage commit against ownership changes.
type StorageLease interface {
	Publish(context.Context, RunScope, ReceiptKey, func() error) error
	RecoverPublication(context.Context, RunScope, ReceiptKey, func() error) error
}

type storageRun struct {
	scope RunScope
	lease StorageLease
}

type storageRunKey struct{}

func WithStorageRun(ctx context.Context, scope RunScope, lease StorageLease) context.Context {
	return context.WithValue(ctx, storageRunKey{}, storageRun{scope: scope, lease: lease})
}

func StorageRun(ctx context.Context, scope RunScope) (StorageLease, bool) {
	run, ok := ctx.Value(storageRunKey{}).(storageRun)
	return run.lease, ok && run.scope == scope && (scope.Epoch == 0) == (run.lease == nil)
}

func WithoutStorageRun(ctx context.Context) context.Context {
	return context.WithValue(ctx, storageRunKey{}, storageRun{})
}
