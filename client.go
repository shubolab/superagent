package superagent

import (
	"context"
	"errors"

	"github.com/shubolab/superagent/internal/mesh"
)

type Client struct {
	transport mesh.MeshTransport
	runner    RunnerRef
}

// NewClient binds the SDK to one runner for all lifecycle operations.
func NewClient(m *Mesh, runner RunnerRef) (*Client, error) {
	if m == nil || m.local == nil {
		return nil, ErrInvalidConfiguration
	}
	if !m.local.HasRunner(runner) {
		return nil, ErrBindingNotFound
	}
	return &Client{transport: m.local, runner: runner}, nil
}

// Start waits for admission before returning a handle to the independent Run.
func (c *Client) Start(ctx context.Context, req StartRequest) (*Run, error) {
	reply, err := c.transport.Call(ctx, mesh.MeshRequest{Operation: mesh.Start, Runner: c.runner, Start: req})
	if errors.Is(err, mesh.ErrResponseLost) {
		return nil, ErrStartOutcomeUnknown
	}
	if err != nil {
		return nil, err
	}
	return c.Run(reply.RunID), nil
}

// Recover resumes a previously admitted recoverable Run on this local runner.
func (c *Client) Recover(ctx context.Context, id RunID) (*Run, error) {
	reply, err := c.transport.Call(ctx, mesh.MeshRequest{Operation: mesh.Recover, Runner: c.runner, RunID: id})
	if err != nil {
		return nil, err
	}
	return c.Run(reply.RunID), nil
}

// Run constructs a handle. The runner checks ownership when the handle is used.
func (c *Client) Run(id RunID) *Run { return &Run{client: c, id: id} }

type Run struct {
	client *Client
	id     RunID
}

func (r *Run) ID() RunID { return r.id }

func (r *Run) Observe(ctx context.Context) (EventStream, error) {
	stream, err := r.client.transport.OpenStream(ctx, r.request(mesh.Observe))
	if err != nil {
		return nil, err
	}
	return mesh.Events(stream), nil
}

func (r *Run) Wait(ctx context.Context) (Result, error) {
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
