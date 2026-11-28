package sdk

import (
	"context"
	"fmt"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
	run "github.com/shubolab/superagent/runner"
)

// MeshConfig supplies the host bindings and policies for a Mesh.
type MeshConfig = mesh.Config

type Mesh struct {
	local *mesh.Local
}

// NewMesh validates and fixes the component bindings for this assembly.
func NewMesh(cfg MeshConfig) (*Mesh, error) {
	models := make(map[contract.ModelRef]struct{}, len(cfg.LocalModels))
	for ref := range cfg.LocalModels {
		models[ref] = struct{}{}
	}
	var local *mesh.Local
	openModel := func(ctx context.Context, subject contract.SubjectID, req contract.ModelRequest) (contract.ModelStream, error) {
		return mesh.OpenModel(ctx, local, subject, req)
	}
	runners := make(map[contract.RunnerRef]mesh.RunnerEndpoint, len(cfg.LocalRunners))
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
		if durable, ok := cfg.LocalArtifacts.(contract.DurableArtifactStore); ok {
			runner.SetArtifactStoreID(durable.ArtifactStoreID())
		}
		if err := runner.ConfigureTools(ref, local, config, cfg.LocalTools, cfg.LocalEnvironments, cfg.LocalArtifacts != nil); err != nil {
			return nil, fmt.Errorf("runner %q: %w", ref, err)
		}
	}
	return &Mesh{local: local}, nil
}
