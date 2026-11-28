package superagent

import (
	"context"

	"github.com/shubolab/superagent/sdk"
)

type Client sdk.Client

// NewClient binds the SDK to one runner for all lifecycle operations.
func NewClient(m *Mesh, runner RunnerRef) (*Client, error) {
	c, err := sdk.NewClient((*sdk.Mesh)(m), runner)
	return (*Client)(c), err
}

// Start waits for admission before returning a handle to the independent Run.
func (c *Client) Start(ctx context.Context, req StartRequest) (*Run, error) {
	run, err := (*sdk.Client)(c).Start(ctx, req)
	return (*Run)(run), err
}

// Recover resumes a previously admitted recoverable Run on this local runner.
func (c *Client) Recover(ctx context.Context, id RunID) (*Run, error) {
	run, err := (*sdk.Client)(c).Recover(ctx, id)
	return (*Run)(run), err
}

// Run constructs a handle. The runner checks ownership when the handle is used.
func (c *Client) Run(id RunID) *Run {
	return (*Run)((*sdk.Client)(c).Run(id))
}

type Run sdk.Run

func (r *Run) ID() RunID { return (*sdk.Run)(r).ID() }

func (r *Run) Observe(ctx context.Context) (EventStream, error) {
	return (*sdk.Run)(r).Observe(ctx)
}

func (r *Run) Wait(ctx context.Context) (Result, error) {
	return (*sdk.Run)(r).Wait(ctx)
}

func (r *Run) Cancel(ctx context.Context) error {
	return (*sdk.Run)(r).Cancel(ctx)
}
