// Package sdk assembles clients and sessions for an embeddable SuperAgent host.
package sdk

import (
	"context"
	"errors"
	"reflect"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

type Client struct {
	transport mesh.MeshTransport
	runner    contract.RunnerRef
}

// NewClient binds the SDK to one runner for all lifecycle operations.
func NewClient(m *Mesh, runner contract.RunnerRef) (*Client, error) {
	if m == nil || m.local == nil {
		return nil, contract.ErrInvalidConfiguration
	}
	if !m.local.HasRunner(runner) {
		return nil, contract.ErrBindingNotFound
	}
	return &Client{transport: m.local, runner: runner}, nil
}

// NewClientWithTransport binds a client to a host-supplied Mesh transport.
// Session operations also require the transport to implement mesh.SessionResolver.
func NewClientWithTransport(transport mesh.MeshTransport, runner contract.RunnerRef) (*Client, error) {
	if nilBinding(transport) || runner == "" {
		return nil, contract.ErrInvalidConfiguration
	}
	if local, ok := transport.(*mesh.Local); ok && !local.HasRunner(runner) {
		return nil, contract.ErrBindingNotFound
	}
	return &Client{transport: transport, runner: runner}, nil
}

func nilBinding(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// Start waits for admission before returning a handle to the independent Run.
func (c *Client) Start(ctx context.Context, req contract.StartRequest) (*Run, error) {
	reply, err := c.transport.Call(ctx, mesh.MeshRequest{Operation: mesh.Start, Runner: c.runner, Start: req})
	if errors.Is(err, mesh.ErrResponseLost) {
		return nil, contract.ErrStartOutcomeUnknown
	}
	if err != nil {
		return nil, err
	}
	return c.Run(reply.RunID), nil
}

// Recover resumes a previously admitted recoverable Run on this local runner.
func (c *Client) Recover(ctx context.Context, id contract.RunID) (*Run, error) {
	reply, err := c.transport.Call(ctx, mesh.MeshRequest{Operation: mesh.Recover, Runner: c.runner, RunID: id})
	if err != nil {
		return nil, err
	}
	return c.Run(reply.RunID), nil
}

// Run constructs a handle. The runner checks ownership when the handle is used.
func (c *Client) Run(id contract.RunID) *Run { return &Run{client: c, id: id} }

type Run struct {
	client *Client
	id     contract.RunID
}

func (r *Run) ID() contract.RunID { return r.id }

func (r *Run) Observe(ctx context.Context) (contract.EventStream, error) {
	stream, err := r.client.transport.OpenStream(ctx, r.request(mesh.Observe))
	if err != nil {
		return nil, err
	}
	return mesh.Events(stream), nil
}

func (r *Run) Wait(ctx context.Context) (contract.Result, error) {
	reply, err := r.client.transport.Call(ctx, r.request(mesh.Wait))
	return reply.Result, err
}

func (r *Run) Cancel(ctx context.Context) error {
	_, err := r.client.transport.Call(ctx, r.request(mesh.Cancel))
	return err
}

func (r *Run) request(operation mesh.Operation) mesh.MeshRequest {
	return mesh.MeshRequest{Operation: operation, Runner: r.client.runner, RunID: r.id}
}
