package model_test

import (
	"context"
	"errors"
	"iter"
	"slices"
	"testing"

	"github.com/shubolab/superagent/model"
)

type catalogWithUnlistedProvider struct{ model.FactoryConfig }

func (catalogWithUnlistedProvider) ProviderRefs() iter.Seq[string] {
	return slices.Values([]string{})
}

func TestModelCatalogRejectsUnlistedProviderBeforeOpening(t *testing.T) {
	cfg := catalogWithUnlistedProvider{model.FactoryConfig{
		Models:    map[model.Ref]model.BindingConfig{"chat": {Provider: "unlisted", ModelID: "model"}},
		Providers: map[string]model.ProviderConfig{"unlisted": {Adapter: "missing"}},
	}}
	bindings, err := model.NewBindings(t.Context(), cfg, nil)
	var setup *model.SetupError
	if bindings != nil || !errors.As(err, &setup) || setup.Code != model.ConfigInvalid ||
		setup.Provider != "unlisted" || setup.Model != "chat" || setup.Field != "provider" {
		t.Fatalf("invalid catalog: bindings=%v, error=%v", bindings, err)
	}
}

type changingCatalog struct {
	model.FactoryConfig
	providerReads int
	modelReads    int
}

func (c *changingCatalog) ProviderConfig(ref string) (model.ProviderConfig, bool) {
	c.providerReads++
	if c.providerReads == 1 {
		return c.FactoryConfig.ProviderConfig(ref)
	}
	return model.ProviderConfig{}, false
}

func (c *changingCatalog) ModelConfig(ref model.Ref) model.BindingConfig {
	c.modelReads++
	if c.modelReads == 1 {
		return c.FactoryConfig.ModelConfig(ref)
	}
	return model.BindingConfig{}
}

type modelClient struct{ modelID string }

func (m *modelClient) Generate(_ context.Context, id string, _ model.Request) (model.Stream, error) {
	m.modelID = id
	return nil, nil
}

func (*modelClient) Close(context.Context) error { return nil }

func TestModelCatalogOpensAndBindsTheValidatedConfiguration(t *testing.T) {
	client := &modelClient{}
	cfg := &changingCatalog{FactoryConfig: model.FactoryConfig{
		Models:    map[model.Ref]model.BindingConfig{"chat": {Provider: "fixture", ModelID: "original"}},
		Providers: map[string]model.ProviderConfig{"fixture": {Adapter: "fixture"}},
		Adapters: map[string]model.Adapter{"fixture": {
			DefaultEndpoint: "https://fixture.invalid",
			Open:            func(context.Context, model.Connection) (model.AdapterClient, error) { return client, nil },
		}},
	}}
	bindings, err := model.NewBindings(t.Context(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bindings.Close(t.Context())
	if _, err := bindings.LocalModels()["chat"].Generate(t.Context(), model.Request{}); err != nil ||
		client.modelID != "original" || cfg.providerReads != 1 || cfg.modelReads != 1 {
		t.Fatalf("model from validated configuration: %q, reads models=%d providers=%d (%v)", client.modelID, cfg.modelReads, cfg.providerReads, err)
	}
}
