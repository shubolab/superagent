// Package mesh routes calls and streams between local component bindings.
package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

type MeshTransport interface {
	Call(ctx context.Context, req MeshRequest) (MeshReply, error)
	OpenStream(ctx context.Context, req MeshRequest) (MeshStream, error)
}

type MeshStream interface {
	Recv() (MeshFrame, error)
	Close() error
}

type Operation uint8

const (
	Start Operation = iota + 1
	Wait
	Cancel
	Observe
	Generate
)

type MeshRequest struct {
	Operation Operation
	Runner    contract.RunnerRef
	RunID     contract.RunID
	Start     contract.StartRequest
	Model     contract.ModelRequest
}

type MeshReply struct {
	RunID  contract.RunID
	Result contract.Result
}

type MeshFrame struct {
	Event      contract.Event
	ModelEvent contract.ModelEvent
}
