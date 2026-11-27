package superagent

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"go.yaml.in/yaml/v3"
)

const maxMeshConfigBytes = 1 << 20

type meshFile struct {
	Version      int                               `yaml:"version"`
	Runners      map[RunnerRef]runnerFile          `yaml:"runners"`
	Models       map[ModelRef]implementation       `yaml:"models"`
	Tools        map[ToolRef]implementation        `yaml:"tools"`
	Environments map[EnvironmentRef]implementation `yaml:"environments"`
}

type runnerFile struct {
	Models             []ModelRef     `yaml:"models"`
	Tools              []ToolRef      `yaml:"tools"`
	DefaultEnvironment EnvironmentRef `yaml:"default_environment"`
}

type implementation struct {
	Local string `yaml:"local"`
}

// LoadMeshConfig resolves logical YAML references against the host's named implementations.
// Runners and the caller resolver are supplied by the host; runner names match the YAML keys.
func LoadMeshConfig(reader io.Reader, catalog MeshConfig) (MeshConfig, error) {
	if reader == nil {
		return MeshConfig{}, fmt.Errorf("%w: missing Mesh configuration", ErrInvalidConfiguration)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxMeshConfigBytes+1))
	if err != nil {
		return MeshConfig{}, fmt.Errorf("%w: reading Mesh configuration: %w", ErrInvalidConfiguration, err)
	}
	if len(data) > maxMeshConfigBytes {
		return MeshConfig{}, fmt.Errorf("%w: Mesh configuration exceeds %d bytes", ErrInvalidConfiguration, maxMeshConfigBytes)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var file meshFile
	if err := dec.Decode(&file); err != nil {
		return MeshConfig{}, fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}
	var second yaml.Node
	if err := dec.Decode(&second); !errors.Is(err, io.EOF) {
		return MeshConfig{}, fmt.Errorf("%w: expected a single Mesh configuration document", ErrInvalidConfiguration)
	}
	if file.Version != 1 || len(file.Runners) == 0 || len(file.Models) == 0 {
		return MeshConfig{}, fmt.Errorf("%w: expected version 1 with runners and models", ErrInvalidConfiguration)
	}
	return file.resolve(catalog)
}

func (f meshFile) resolve(catalog MeshConfig) (MeshConfig, error) {
	models, err := resolveImplementations("model", f.Models, catalog.LocalModels)
	if err != nil {
		return MeshConfig{}, err
	}
	tools, err := resolveImplementations("tool", f.Tools, catalog.LocalTools)
	if err != nil {
		return MeshConfig{}, err
	}
	environments, err := resolveImplementations("environment", f.Environments, catalog.LocalEnvironments)
	if err != nil {
		return MeshConfig{}, err
	}
	cfg := MeshConfig{LocalRunners: make(map[RunnerRef]RunnerConfig, len(f.Runners)),
		LocalModels: models, LocalTools: tools, LocalEnvironments: environments, ResolveCaller: catalog.ResolveCaller}
	for _, ref := range slices.Sorted(maps.Keys(f.Runners)) {
		decl := f.Runners[ref]
		runner, ok := catalog.LocalRunners[ref]
		if !ok || ref == "" {
			return MeshConfig{}, fmt.Errorf("%w: runner %q has no host policy", ErrInvalidConfiguration, ref)
		}
		runner, err = resolveRunner(ref, runner, decl, cfg)
		if err != nil {
			return MeshConfig{}, err
		}
		cfg.LocalRunners[ref] = runner
	}
	return cfg, nil
}

func resolveImplementations[Ref ~string, Impl any](kind string, refs map[Ref]implementation, catalog map[Ref]Impl) (map[Ref]Impl, error) {
	bindings := make(map[Ref]Impl, len(refs))
	for _, ref := range slices.Sorted(maps.Keys(refs)) {
		local := refs[ref].Local
		impl, ok := catalog[Ref(local)]
		if !ok || ref == "" || local == "" {
			return nil, fmt.Errorf("%w: %s %q has no %s implementation %q", ErrInvalidConfiguration, kind, ref, kind, local)
		}
		bindings[ref] = impl
	}
	return bindings, nil
}

func resolveRunner(ref RunnerRef, runner RunnerConfig, decl runnerFile, cfg MeshConfig) (RunnerConfig, error) {
	models, err := visibleBindings(decl.Models, cfg.LocalModels)
	if err != nil {
		return RunnerConfig{}, fmt.Errorf("runner %q models: %w", ref, err)
	}
	tools, err := visibleBindings(decl.Tools, cfg.LocalTools)
	if err != nil {
		return RunnerConfig{}, fmt.Errorf("runner %q tools: %w", ref, err)
	}
	for _, allowed := range runner.AllowedModels {
		for _, model := range allowed {
			if !models[model] {
				return RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared model %q", ErrInvalidConfiguration, ref, model)
			}
		}
	}
	for _, allowed := range runner.AllowedTools {
		for tool, permission := range allowed {
			if !tools[tool] {
				return RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared tool %q", ErrInvalidConfiguration, ref, tool)
			}
			if env := permission.Environment.Ref; env != "" && cfg.LocalEnvironments[env] == nil {
				return RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared environment %q", ErrInvalidConfiguration, ref, env)
			}
		}
	}
	if env := decl.DefaultEnvironment; env != "" && cfg.LocalEnvironments[env] == nil {
		return RunnerConfig{}, fmt.Errorf("%w: runner %q declares unknown default environment %q", ErrInvalidConfiguration, ref, env)
	}
	runner.DefaultEnvironment = decl.DefaultEnvironment
	return runner, nil
}

func visibleBindings[Ref ~string, Impl any](refs []Ref, registered map[Ref]Impl) (map[Ref]bool, error) {
	visible := make(map[Ref]bool, len(refs))
	for _, ref := range refs {
		_, exists := registered[ref]
		if !exists || visible[ref] {
			return nil, fmt.Errorf("%w: missing or repeated binding %q", ErrInvalidConfiguration, ref)
		}
		visible[ref] = true
	}
	return visible, nil
}
