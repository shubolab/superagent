package model

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

type SetupCode string

const (
	ConfigInvalid         SetupCode = "model_config_invalid"
	CredentialUnavailable SetupCode = "model_credential_unavailable"
	ClientInitFailed      SetupCode = "model_client_init_failed"
)

type SetupError struct {
	Code     SetupCode
	Provider string
	Model    Ref
	Field    string
}

func (e *SetupError) Error() string {
	return fmt.Sprintf("%s (provider %q, model %q, field %s)", e.Code, e.Provider, e.Model, e.Field)
}

const modelCleanupTimeout = time.Second

var ErrCloseFailed = errors.New("model_client_close_failed")

type Bindings struct {
	models  map[Ref]Provider
	clients []AdapterClient

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

// NewBindings fixes a set of model routes without sending a model request.
func NewBindings(ctx context.Context, cfg Catalog, secrets CredentialSource) (*Bindings, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nilModelComponent(cfg) {
		return nil, &SetupError{Code: ConfigInvalid, Field: "models"}
	}
	providers, err := validateCatalog(cfg, slices.Sorted(cfg.ModelRefs()))
	if err != nil {
		return nil, err
	}
	b := &Bindings{models: make(map[Ref]Provider), closed: make(chan struct{})}
	clients := make(map[string]AdapterClient, len(providers))
	for _, ref := range providers {
		provider, _ := cfg.ProviderConfig(ref)
		adapter, _ := cfg.AdapterConfig(provider.Adapter)
		client, err := openProvider(ctx, ref, provider, adapter, secrets)
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
	for ref := range cfg.ModelRefs() {
		config := cfg.ModelConfig(ref)
		base := boundModel{client: clients[config.Provider], modelID: config.ModelID}
		b.models[ref] = base
		if provider, ok := base.client.(AdapterInputMeters); ok {
			if meter := provider.InputMeter(config.ModelID); !nilModelComponent(meter) {
				b.models[ref] = meteredBoundModel{boundModel: base, InputMeter: meter}
			}
		}
	}
	return b, nil
}

func validateCatalog(cfg Catalog, models []Ref) ([]string, error) {
	if len(models) == 0 {
		return nil, &SetupError{Code: ConfigInvalid, Field: "models"}
	}
	for _, ref := range slices.Sorted(cfg.ProviderRefs()) {
		p, registered := cfg.ProviderConfig(ref)
		adapter, ok := cfg.AdapterConfig(p.Adapter)
		if !registered || strings.TrimSpace(ref) == "" || p.Adapter == "" || !ok || adapter.Open == nil {
			return nil, &SetupError{Code: ConfigInvalid, Provider: ref, Field: "adapter"}
		}
		if !validModelEndpoint(modelEndpoint(p, adapter)) {
			return nil, &SetupError{Code: ConfigInvalid, Provider: ref, Field: "endpoint"}
		}
	}
	used := make(map[string]struct{})
	for _, ref := range models {
		binding := cfg.ModelConfig(ref)
		if strings.TrimSpace(string(ref)) == "" || strings.TrimSpace(binding.ModelID) == "" {
			return nil, &SetupError{Code: ConfigInvalid, Model: ref, Field: "model_id"}
		}
		if _, ok := cfg.ProviderConfig(binding.Provider); !ok {
			return nil, &SetupError{Code: ConfigInvalid, Provider: binding.Provider, Model: ref, Field: "provider"}
		}
		used[binding.Provider] = struct{}{}
	}
	return slices.Sorted(maps.Keys(used)), nil
}

func modelEndpoint(cfg ProviderConfig, adapter Adapter) string {
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

func openProvider(ctx context.Context, ref string, cfg ProviderConfig, adapter Adapter, secrets CredentialSource) (AdapterClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var credential string
	if cfg.CredentialRef != "" || adapter.RequiresCredential {
		if cfg.CredentialRef == "" || nilModelComponent(secrets) {
			return nil, &SetupError{Code: CredentialUnavailable, Provider: ref, Field: "credential_ref"}
		}
		var err error
		credential, err = secrets.Resolve(ctx, cfg.CredentialRef)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil || strings.TrimSpace(credential) == "" {
			return nil, &SetupError{Code: CredentialUnavailable, Provider: ref, Field: "credential_ref"}
		}
	}
	client, err := adapter.Open(ctx, Connection{Endpoint: modelEndpoint(cfg, adapter), Credential: credential})
	if ctx.Err() != nil {
		return client, ctx.Err()
	}
	if err != nil || nilModelComponent(client) {
		return client, &SetupError{Code: ClientInitFailed, Provider: ref, Field: "adapter"}
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
func (b *Bindings) LocalModels() map[Ref]Provider {
	return maps.Clone(b.models)
}

// Close releases the clients once. The caller's context controls its wait for cleanup.
func (b *Bindings) Close(ctx context.Context) error {
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

func (b *Bindings) release() {
	defer close(b.closed)
	ctx, cancel := context.WithTimeout(context.Background(), modelCleanupTimeout)
	defer cancel()
	for i := len(b.clients) - 1; i >= 0; i-- {
		if err := b.clients[i].Close(ctx); err != nil {
			b.closeErr = ErrCloseFailed
		}
	}
}

type boundModel struct {
	client  AdapterClient
	modelID string
}

func (m boundModel) Generate(ctx context.Context, req Request) (Stream, error) {
	return m.client.Generate(ctx, m.modelID, req)
}

type meteredBoundModel struct {
	boundModel
	InputMeter
}
