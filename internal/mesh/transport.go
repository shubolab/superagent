// Package mesh routes calls and streams between local component bindings.
package mesh

import (
	"context"
	"io"

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
	Invoke
	AllocateEnvironment
	GrantEnvironment
	OpenEnvironmentRead
	RevokeEnvironment
	ReleaseEnvironment
)

type MeshRequest struct {
	Operation   Operation
	Runner      contract.RunnerRef
	RunID       contract.RunID
	Start       contract.StartRequest
	Model       contract.ModelRequest
	Tool        contract.ToolRequest
	Environment EnvironmentRequest
}

type MeshReply struct {
	RunID   contract.RunID
	Result  contract.Result
	Tool    contract.ToolResult
	Handle  contract.EnvironmentHandle
	Grant   contract.EnvironmentGrantRef
	Reader  io.ReadCloser
	Scanned uint64
}

type MeshFrame struct {
	Event      contract.Event
	ModelEvent contract.ModelEvent
}

type EnvironmentRequest struct {
	Ref    contract.EnvironmentRef
	Run    contract.RunScope
	Call   contract.CallScope
	Handle contract.EnvironmentHandle
	Grant  contract.EnvironmentGrantRef
	Path   string
	Prefix string
}
