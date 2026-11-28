package run

import (
	"context"
	"path/filepath"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
)

type RemoteAuthority struct{ directory string }

func NewRemoteAuthority(directory string) (*RemoteAuthority, error) {
	if !durable.Supported {
		return nil, contract.ErrInvalidConfiguration
	}
	if err := durable.Directory(directory); err != nil {
		return nil, err
	}
	return &RemoteAuthority{directory}, nil
}

func (a *RemoteAuthority) lease(key contract.ReceiptKey, owner contract.RemoteOwner) *lease {
	id := runIdentity{Runner: key.Runner, Subject: key.Subject, ID: key.RunID}
	digest, _ := durable.Digest(id)
	return &lease{path: filepath.Join(a.directory, digest+".run"), id: id,
		owner: ownership{Instance: owner.Instance, Epoch: owner.Epoch}, clock: func() (time.Time, error) { return time.Now(), nil }}
}

func (a *RemoteAuthority) AdmitRemote(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner, req contract.ToolRequest, path string) error {
	e := mesh.EnvironmentRequest{Ref: key.Environment, Run: scope,
		Call: contract.CallScope{Run: scope, CallID: key.CallID, Tool: key.Tool, Path: path, Limits: req.Limits}}
	return a.lease(key, owner).admit(ctx, mesh.MeshRequest{Operation: mesh.Invoke, Environment: e, Tool: req, ReceiptKey: key}, true)
}

func (a *RemoteAuthority) ReadRemote(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner, workspace bool) error {
	l := a.lease(key, owner)
	if !workspace {
		return l.admit(ctx, mesh.MeshRequest{Operation: mesh.QueryReceipt, ReceiptKey: key,
			Environment: mesh.EnvironmentRequest{Ref: key.Environment, Run: scope}}, true)
	}
	return l.owned(ctx, func(c *checkpoint, _ time.Time) error {
		if !l.Matches(scope) || c.RemoteBindings[key.Tool] != key.Remote ||
			c.Stop != nil || !scope.Deadline.Equal(c.Deadline) {
			return contract.ErrAccessDenied
		}
		return nil
	})
}

func (a *RemoteAuthority) PublishRemote(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner, publish func() error) error {
	return a.lease(key, owner).Publish(ctx, scope, key, publish)
}
