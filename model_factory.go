package superagent

import (
	"context"
	"fmt"
	"iter"
	"maps"

	"github.com/shubolab/superagent/model"
)

type ProviderRef string

type ModelFactoryConfig struct {
	Providers map[ProviderRef]ProviderConfig
	Models    map[ModelRef]ModelBindingConfig
	Adapters  map[string]ModelAdapter
}

type ProviderConfig struct {
	Adapter       string
	Endpoint      string
	CredentialRef string
}

type ModelBindingConfig struct {
	Provider ProviderRef
	ModelID  string
}

type CredentialSource interface {
	Resolve(ctx context.Context, ref string) (string, error)
}

type ModelAdapter struct {
	DefaultEndpoint    string
	RequiresCredential bool
	Open               func(context.Context, ProviderConnection) (ModelAdapterClient, error)
}

type ProviderConnection struct {
	Endpoint   string
	Credential string
}

// ModelAdapterClient is shared by all logical models bound to one ProviderRef.
// Generate must support independent concurrent request contexts.
type ModelAdapterClient interface {
	Generate(ctx context.Context, modelID string, req ModelRequest) (ModelStream, error)
	Close(ctx context.Context) error
}

// ModelAdapterInputMeters is an optional capability of a factory client.
type ModelAdapterInputMeters interface {
	InputMeter(modelID string) ModelInputMeter
}

type ModelSetupCode string

const (
	ModelConfigInvalid         ModelSetupCode = "model_config_invalid"
	ModelCredentialUnavailable ModelSetupCode = "model_credential_unavailable"
	ModelClientInitFailed      ModelSetupCode = "model_client_init_failed"
)

type ModelSetupError struct {
	Code     ModelSetupCode
	Provider ProviderRef
	Model    ModelRef
	Field    string
}

func (e *ModelSetupError) Error() string {
	return fmt.Sprintf("%s (provider %q, model %q, field %s)", e.Code, e.Provider, e.Model, e.Field)
}

var ErrModelCloseFailed = model.ErrCloseFailed

type ModelBindings model.Bindings

// NewModelBindings fixes a set of model routes without sending a model request.
func NewModelBindings(ctx context.Context, cfg ModelFactoryConfig, secrets CredentialSource) (*ModelBindings, error) {
	bindings, err := model.NewBindings(ctx, modelCatalog(cfg), secrets)
	if setup, ok := err.(*model.SetupError); ok {
		err = &ModelSetupError{
			Code: ModelSetupCode(setup.Code), Provider: ProviderRef(setup.Provider), Model: setup.Model, Field: setup.Field,
		}
	}
	return (*ModelBindings)(bindings), err
}

// LocalModels returns a copy of the registrations for use with NewMesh.
func (b *ModelBindings) LocalModels() map[ModelRef]ModelProvider {
	return (*model.Bindings)(b).LocalModels()
}

// Close releases the clients once. The caller's context controls its wait for cleanup.
func (b *ModelBindings) Close(ctx context.Context) error {
	return (*model.Bindings)(b).Close(ctx)
}

type modelCatalog ModelFactoryConfig

func (c modelCatalog) ProviderRefs() iter.Seq[string] {
	return func(yield func(string) bool) {
		for ref := range c.Providers {
			if !yield(string(ref)) {
				return
			}
		}
	}
}

func (c modelCatalog) ModelRefs() iter.Seq[ModelRef] {
	return maps.Keys(c.Models)
}

func (c modelCatalog) ProviderConfig(ref string) (model.ProviderConfig, bool) {
	p, ok := c.Providers[ProviderRef(ref)]
	return model.ProviderConfig(p), ok
}

func (c modelCatalog) ModelConfig(ref ModelRef) model.BindingConfig {
	b := c.Models[ref]
	return model.BindingConfig{Provider: string(b.Provider), ModelID: b.ModelID}
}

func (c modelCatalog) AdapterConfig(name string) (model.Adapter, bool) {
	a, ok := c.Adapters[name]
	adapter := model.Adapter{DefaultEndpoint: a.DefaultEndpoint, RequiresCredential: a.RequiresCredential}
	if a.Open != nil {
		adapter.Open = func(ctx context.Context, connection model.Connection) (model.AdapterClient, error) {
			return a.Open(ctx, ProviderConnection(connection))
		}
	}
	return adapter, ok
}
