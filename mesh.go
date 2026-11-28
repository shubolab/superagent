package superagent

import (
	"context"
	"fmt"
	"io"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/run"
)

// MeshConfig supplies the host bindings and policies for a Mesh.
type MeshConfig struct {
	LocalRunners      map[RunnerRef]RunnerConfig
	LocalModels       map[ModelRef]ModelProvider
	LocalTools        map[ToolRef]LocalToolBinding
	LocalEnvironments map[EnvironmentRef]EnvironmentProvider
	ResolveCaller     func(context.Context) (SubjectID, error)
	LocalArtifacts    ArtifactStore
}

// LoadMeshConfig resolves logical YAML references against the host's named implementations.
// Runners and the caller resolver are supplied by the host; runner names match the YAML keys.
func LoadMeshConfig(reader io.Reader, catalog MeshConfig) (MeshConfig, error) {
	cfg, err := mesh.LoadConfig(reader, mesh.Config(catalog))
	return MeshConfig(cfg), err
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
		if durable, ok := cfg.LocalArtifacts.(contract.DurableArtifactStore); ok {
			runner.SetArtifactStoreID(durable.ArtifactStoreID())
		}
		if err := runner.ConfigureTools(ref, local, config, cfg.LocalTools, cfg.LocalEnvironments, cfg.LocalArtifacts != nil); err != nil {
			return nil, fmt.Errorf("runner %q: %w", ref, err)
		}
	}
	return &Mesh{local: local}, nil
}
