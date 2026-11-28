// Package model assembles provider clients and exposes them as model bindings.
package model

import (
	"context"
	"iter"
	"maps"

	"github.com/shubolab/superagent/internal/contract"
)

type Ref = contract.ModelRef
type Provider = contract.ModelProvider
type Request = contract.ModelRequest
type Stream = contract.ModelStream
type Event = contract.ModelEvent
type Reply = contract.ModelReply
type InputMeter = contract.ModelInputMeter
type ToolCall = contract.ModelToolCall

const (
	TextDelta       = contract.ModelTextDelta
	Completed       = contract.ModelCompleted
	FinishStop      = contract.ModelFinishStop
	FinishToolCalls = contract.ModelFinishToolCalls
)

// Catalog supplies the provider, model and adapter registrations for assembly.
// ProviderRefs must include every provider referenced by a model.
type Catalog interface {
	ProviderRefs() iter.Seq[string]
	ModelRefs() iter.Seq[Ref]
	ProviderConfig(ref string) (ProviderConfig, bool)
	ModelConfig(ref Ref) BindingConfig
	AdapterConfig(name string) (Adapter, bool)
}

// FactoryConfig is a catalog backed by host-provided registrations.
type FactoryConfig struct {
	Providers map[string]ProviderConfig
	Models    map[Ref]BindingConfig
	Adapters  map[string]Adapter
}

func (c FactoryConfig) ProviderRefs() iter.Seq[string]    { return maps.Keys(c.Providers) }
func (c FactoryConfig) ModelRefs() iter.Seq[Ref]          { return maps.Keys(c.Models) }
func (c FactoryConfig) ModelConfig(ref Ref) BindingConfig { return c.Models[ref] }
func (c FactoryConfig) ProviderConfig(ref string) (ProviderConfig, bool) {
	provider, ok := c.Providers[ref]
	return provider, ok
}

func (c FactoryConfig) AdapterConfig(name string) (Adapter, bool) {
	adapter, ok := c.Adapters[name]
	return adapter, ok
}

type ProviderConfig struct {
	Adapter       string
	Endpoint      string
	CredentialRef string
}

type BindingConfig struct {
	Provider string
	ModelID  string
}

type CredentialSource interface {
	Resolve(ctx context.Context, ref string) (string, error)
}

type Adapter struct {
	DefaultEndpoint    string
	RequiresCredential bool
	Open               func(context.Context, Connection) (AdapterClient, error)
}

type Connection struct {
	Endpoint   string
	Credential string
}

// AdapterClient is shared by the logical models bound to one provider.
// Generate must support independent concurrent request contexts.
type AdapterClient interface {
	Generate(ctx context.Context, modelID string, req Request) (Stream, error)
	Close(ctx context.Context) error
}

// AdapterInputMeters is an optional capability of an adapter client.
type AdapterInputMeters interface {
	InputMeter(modelID string) InputMeter
}
