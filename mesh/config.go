package mesh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"go.yaml.in/yaml/v3"

	"github.com/shubolab/superagent/internal/contract"
)

const maxMeshConfigBytes = 1 << 20

type Config struct {
	LocalRunners      map[contract.RunnerRef]contract.RunnerConfig
	LocalModels       map[contract.ModelRef]contract.ModelProvider
	LocalTools        map[contract.ToolRef]contract.LocalToolBinding
	LocalEnvironments map[contract.EnvironmentRef]contract.EnvironmentProvider
	ResolveCaller     func(context.Context) (contract.SubjectID, error)
	LocalArtifacts    contract.ArtifactStore
}

type meshFile struct {
	Version      int                                        `yaml:"version"`
	Runners      map[contract.RunnerRef]runnerFile          `yaml:"runners"`
	Models       map[contract.ModelRef]implementation       `yaml:"models"`
	Tools        map[contract.ToolRef]implementation        `yaml:"tools"`
	Environments map[contract.EnvironmentRef]implementation `yaml:"environments"`
}

type runnerFile struct {
	Models             []contract.ModelRef     `yaml:"models"`
	Tools              []contract.ToolRef      `yaml:"tools"`
	DefaultEnvironment contract.EnvironmentRef `yaml:"default_environment"`
}

type implementation struct {
	Local string `yaml:"local"`
}

// LoadConfig resolves YAML references against host-provided bindings.
func LoadConfig(reader io.Reader, catalog Config) (Config, error) {
	if reader == nil {
		return Config{}, fmt.Errorf("%w: missing Mesh configuration", contract.ErrInvalidConfiguration)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxMeshConfigBytes+1))
	if err != nil {
		return Config{}, fmt.Errorf("%w: reading Mesh configuration: %w", contract.ErrInvalidConfiguration, err)
	}
	if len(data) > maxMeshConfigBytes {
		return Config{}, fmt.Errorf("%w: Mesh configuration exceeds %d bytes", contract.ErrInvalidConfiguration, maxMeshConfigBytes)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var file meshFile
	if err := dec.Decode(&file); err != nil {
		return Config{}, fmt.Errorf("%w: %v", contract.ErrInvalidConfiguration, err)
	}
	var second yaml.Node
	if err := dec.Decode(&second); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("%w: expected a single Mesh configuration document", contract.ErrInvalidConfiguration)
	}
	if file.Version != 1 || len(file.Runners) == 0 || len(file.Models) == 0 {
		return Config{}, fmt.Errorf("%w: expected version 1 with runners and models", contract.ErrInvalidConfiguration)
	}
	return file.resolve(catalog)
}

func (f meshFile) resolve(catalog Config) (Config, error) {
	models, err := resolveImplementations("model", f.Models, catalog.LocalModels)
	if err != nil {
		return Config{}, err
	}
	tools, err := resolveImplementations("tool", f.Tools, catalog.LocalTools)
	if err != nil {
		return Config{}, err
	}
	environments, err := resolveImplementations("environment", f.Environments, catalog.LocalEnvironments)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{LocalRunners: make(map[contract.RunnerRef]contract.RunnerConfig, len(f.Runners)),
		LocalModels: models, LocalTools: tools, LocalEnvironments: environments, ResolveCaller: catalog.ResolveCaller, LocalArtifacts: catalog.LocalArtifacts}
	for _, ref := range slices.Sorted(maps.Keys(f.Runners)) {
		decl := f.Runners[ref]
		runner, ok := catalog.LocalRunners[ref]
		if !ok || ref == "" {
			return Config{}, fmt.Errorf("%w: runner %q has no host policy", contract.ErrInvalidConfiguration, ref)
		}
		runner, err = resolveRunner(ref, runner, decl, cfg)
		if err != nil {
			return Config{}, err
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
			return nil, fmt.Errorf("%w: %s %q has no %s implementation %q", contract.ErrInvalidConfiguration, kind, ref, kind, local)
		}
		bindings[ref] = impl
	}
	return bindings, nil
}

func resolveRunner(ref contract.RunnerRef, runner contract.RunnerConfig, decl runnerFile, cfg Config) (contract.RunnerConfig, error) {
	models, err := visibleBindings(decl.Models, cfg.LocalModels)
	if err != nil {
		return contract.RunnerConfig{}, fmt.Errorf("runner %q models: %w", ref, err)
	}
	tools, err := visibleBindings(decl.Tools, cfg.LocalTools)
	if err != nil {
		return contract.RunnerConfig{}, fmt.Errorf("runner %q tools: %w", ref, err)
	}
	for _, allowed := range runner.AllowedModels {
		for _, model := range allowed {
			if !models[model] {
				return contract.RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared model %q", contract.ErrInvalidConfiguration, ref, model)
			}
		}
	}
	for _, allowed := range runner.AllowedTools {
		for tool, permission := range allowed {
			if !tools[tool] {
				return contract.RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared tool %q", contract.ErrInvalidConfiguration, ref, tool)
			}
			if env := permission.Environment.Ref; env != "" && cfg.LocalEnvironments[env] == nil {
				return contract.RunnerConfig{}, fmt.Errorf("%w: runner %q policy grants undeclared environment %q", contract.ErrInvalidConfiguration, ref, env)
			}
		}
	}
	if env := decl.DefaultEnvironment; env != "" && cfg.LocalEnvironments[env] == nil {
		return contract.RunnerConfig{}, fmt.Errorf("%w: runner %q declares unknown default environment %q", contract.ErrInvalidConfiguration, ref, env)
	}
	runner.DefaultEnvironment = decl.DefaultEnvironment
	return runner, nil
}

func visibleBindings[Ref ~string, Impl any](refs []Ref, registered map[Ref]Impl) (map[Ref]bool, error) {
	visible := make(map[Ref]bool, len(refs))
	for _, ref := range refs {
		_, exists := registered[ref]
		if !exists || visible[ref] {
			return nil, fmt.Errorf("%w: missing or repeated binding %q", contract.ErrInvalidConfiguration, ref)
		}
		visible[ref] = true
	}
	return visible, nil
}
