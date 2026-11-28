package superagent

import (
	"context"
	"io"

	"github.com/shubolab/superagent/mesh"
	"github.com/shubolab/superagent/sdk"
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

type Mesh sdk.Mesh

// NewMesh validates and fixes the component bindings for this assembly.
func NewMesh(cfg MeshConfig) (*Mesh, error) {
	m, err := sdk.NewMesh(sdk.MeshConfig(cfg))
	return (*Mesh)(m), err
}
