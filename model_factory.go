package superagent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
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

const modelCleanupTimeout = time.Second

var ErrModelCloseFailed = errors.New("model_client_close_failed")

type ModelBindings struct {
	models  map[ModelRef]ModelProvider
	clients []ModelAdapterClient

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

// NewModelBindings fixes a set of model routes without sending a model request.
func NewModelBindings(ctx context.Context, cfg ModelFactoryConfig, secrets CredentialSource) (*ModelBindings, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	providers, err := validateModelFactory(cfg)
	if err != nil {
		return nil, err
	}
	b := &ModelBindings{models: make(map[ModelRef]ModelProvider, len(cfg.Models)), closed: make(chan struct{})}
	clients := make(map[ProviderRef]ModelAdapterClient, len(providers))
	for _, ref := range providers {
		client, err := openProvider(ctx, ref, cfg.Providers[ref], cfg.Adapters[cfg.Providers[ref].Adapter], secrets)
		if !nilModelComponent(client) {
			b.clients = append(b.clients, client)
		}
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), modelCleanupTimeout)
			_ = b.Close(cleanup)
			cancel()
			return nil, err
		}
		clients[ref] = client
	}
	for ref, config := range cfg.Models {
		b.models[ref] = boundModel{client: clients[config.Provider], modelID: config.ModelID}
	}
	return b, nil
}

func validateModelFactory(cfg ModelFactoryConfig) ([]ProviderRef, error) {
	if len(cfg.Models) == 0 {
		return nil, &ModelSetupError{Code: ModelConfigInvalid, Field: "models"}
	}
	for _, ref := range slices.Sorted(maps.Keys(cfg.Providers)) {
		p := cfg.Providers[ref]
		adapter, ok := cfg.Adapters[p.Adapter]
		if strings.TrimSpace(string(ref)) == "" || p.Adapter == "" || !ok || adapter.Open == nil {
			return nil, &ModelSetupError{Code: ModelConfigInvalid, Provider: ref, Field: "adapter"}
		}
		if !validModelEndpoint(modelEndpoint(p, adapter)) {
			return nil, &ModelSetupError{Code: ModelConfigInvalid, Provider: ref, Field: "endpoint"}
		}
	}
	used := make(map[ProviderRef]struct{})
	for _, ref := range slices.Sorted(maps.Keys(cfg.Models)) {
		binding := cfg.Models[ref]
		if strings.TrimSpace(string(ref)) == "" || strings.TrimSpace(binding.ModelID) == "" {
			return nil, &ModelSetupError{Code: ModelConfigInvalid, Model: ref, Field: "model_id"}
		}
		if _, ok := cfg.Providers[binding.Provider]; !ok {
			return nil, &ModelSetupError{Code: ModelConfigInvalid, Provider: binding.Provider, Model: ref, Field: "provider"}
		}
		used[binding.Provider] = struct{}{}
	}
	return slices.Sorted(maps.Keys(used)), nil
}

func modelEndpoint(cfg ProviderConfig, adapter ModelAdapter) string {
	if cfg.Endpoint != "" {
		return cfg.Endpoint
	}
	return adapter.DefaultEndpoint
}

func validModelEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && u.Hostname() != "" && (u.Scheme == "https" || u.Scheme == "http") &&
		u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == ""
}

func openProvider(ctx context.Context, ref ProviderRef, cfg ProviderConfig, adapter ModelAdapter, secrets CredentialSource) (ModelAdapterClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var credential string
	if cfg.CredentialRef != "" || adapter.RequiresCredential {
		if cfg.CredentialRef == "" || nilModelComponent(secrets) {
			return nil, &ModelSetupError{Code: ModelCredentialUnavailable, Provider: ref, Field: "credential_ref"}
		}
		var err error
		credential, err = secrets.Resolve(ctx, cfg.CredentialRef)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil || strings.TrimSpace(credential) == "" {
			return nil, &ModelSetupError{Code: ModelCredentialUnavailable, Provider: ref, Field: "credential_ref"}
		}
	}
	client, err := adapter.Open(ctx, ProviderConnection{Endpoint: modelEndpoint(cfg, adapter), Credential: credential})
	if ctx.Err() != nil {
		return client, ctx.Err()
	}
	if err != nil || nilModelComponent(client) {
		return client, &ModelSetupError{Code: ModelClientInitFailed, Provider: ref, Field: "adapter"}
	}
	return client, nil
}

func nilModelComponent(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// LocalModels returns a copy of the registrations for use with NewMesh.
func (b *ModelBindings) LocalModels() map[ModelRef]ModelProvider {
	return maps.Clone(b.models)
}

// Close releases the clients once. The caller's context controls its wait for cleanup.
func (b *ModelBindings) Close(ctx context.Context) error {
	if len(b.clients) == 0 {
		return nil
	}
	b.closeOnce.Do(func() {
		go b.release()
	})
	select {
	case <-b.closed:
		return b.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *ModelBindings) release() {
	defer close(b.closed)
	ctx, cancel := context.WithTimeout(context.Background(), modelCleanupTimeout)
	defer cancel()
	for i := len(b.clients) - 1; i >= 0; i-- {
		if err := b.clients[i].Close(ctx); err != nil {
			b.closeErr = ErrModelCloseFailed
		}
	}
}

type boundModel struct {
	client  ModelAdapterClient
	modelID string
}

func (m boundModel) Generate(ctx context.Context, req ModelRequest) (ModelStream, error) {
	return m.client.Generate(ctx, m.modelID, req)
}
