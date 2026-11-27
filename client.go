package superagent

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/shubolab/superagent/internal/contract"

	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/run"
)

type MeshConfig struct {
	LocalRunners      map[RunnerRef]RunnerConfig
	LocalModels       map[ModelRef]ModelProvider
	LocalTools        map[ToolRef]LocalToolBinding
	LocalEnvironments map[EnvironmentRef]EnvironmentProvider
	ResolveCaller     func(context.Context) (SubjectID, error)
	LocalArtifacts    ArtifactStore
}

type Mesh struct {
	local *mesh.Local
}

// NewMesh validates and fixes the component bindings for this assembly.
func NewMesh(cfg MeshConfig) (*Mesh, error) {
	models := make(map[ModelRef]struct{}, len(cfg.LocalModels))
	for ref := range cfg.LocalModels {
		models[ref] = struct{}{}
	}
	var local *mesh.Local
	openModel := func(ctx context.Context, subject SubjectID, req ModelRequest) (ModelStream, error) {
		return mesh.OpenModel(ctx, local, subject, req)
	}
	runners := make(map[RunnerRef]mesh.RunnerEndpoint, len(cfg.LocalRunners))
	for ref, config := range cfg.LocalRunners {
		runner, err := run.New(config, models, openModel)
		if err != nil {
			return nil, fmt.Errorf("runner %q: %w", ref, err)
		}
		runners[ref] = runner
	}
	var err error
	local, err = mesh.NewLocal(runners, cfg.LocalModels, cfg.ResolveCaller)
	if err != nil {
		return nil, err
	}
	if err := local.SetComponents(cfg.LocalTools, cfg.LocalEnvironments); err != nil {
		return nil, err
	}
	if err := local.SetArtifactStore(cfg.LocalArtifacts); err != nil {
		return nil, err
	}
	for ref, config := range cfg.LocalRunners {
		runner := runners[ref].(*run.Runner)
		if retained, ok := cfg.LocalArtifacts.(contract.RetainedArtifactStore); ok {
			runner.SetRetainedArtifacts(retained.RetainsAcrossRestart())
		}
		if staging, ok := cfg.LocalArtifacts.(contract.TakeoverArtifactStore); ok {
			runner.SetTakeoverArtifacts(staging.StagesUntilPublication())
		}
		if err := runner.ConfigureTools(ref, local, config, cfg.LocalTools, cfg.LocalEnvironments, cfg.LocalArtifacts != nil); err != nil {
			return nil, fmt.Errorf("runner %q: %w", ref, err)
		}
	}
	return &Mesh{local: local}, nil
}

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

// OpenToolFile uses the workspace permission carried with an admitted tool invocation.
func OpenToolFile(ctx context.Context, req ToolRequest, path string) (io.ReadCloser, error) {
	return contract.OpenToolFile(ctx, req, path)
}
