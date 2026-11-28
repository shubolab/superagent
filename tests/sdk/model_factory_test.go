package superagent_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shubolab/superagent"
)

type credentialFunc func(context.Context, string) (string, error)

func (s credentialFunc) Resolve(ctx context.Context, ref string) (string, error) { return s(ctx, ref) }

type factoryClient struct {
	generate func(context.Context, string, superagent.ModelRequest) (superagent.ModelStream, error)
	close    func(context.Context) error
}

func (c *factoryClient) Generate(ctx context.Context, id string, req superagent.ModelRequest) (superagent.ModelStream, error) {
	return c.generate(ctx, id, req)
}

func (c *factoryClient) Close(ctx context.Context) error { return c.close(ctx) }

type recordedModel struct {
	modelID string
	req     superagent.ModelRequest
	ctx     context.Context
}

func twoModelConfig(open func(context.Context, superagent.ProviderConnection) (superagent.ModelAdapterClient, error)) superagent.ModelFactoryConfig {
	return superagent.ModelFactoryConfig{
		Providers: map[superagent.ProviderRef]superagent.ProviderConfig{
			"primary": {Adapter: "local-test", CredentialRef: "secret/primary"},
		},
		Models: map[superagent.ModelRef]superagent.ModelBindingConfig{
			"chat": {Provider: "primary", ModelID: "chat-v1"},
			"fast": {Provider: "primary", ModelID: "fast-v2"},
		},
		Adapters: map[string]superagent.ModelAdapter{
			"local-test": {RequiresCredential: true, DefaultEndpoint: "https://provider.example", Open: open},
		},
	}
}

func clientForModels(t *testing.T, bindings *superagent.ModelBindings) *superagent.Client {
	t.Helper()
	cfg := hostConfig(nil)
	cfg.LocalModels = bindings.LocalModels()
	runner := cfg.LocalRunners["main"]
	runner.AllowedModels["alice"] = []superagent.ModelRef{"chat", "fast"}
	runner.AllowedModels["bob"] = nil
	cfg.LocalRunners["main"] = runner
	return newClient(t, cfg)
}

func TestModelFactorySharesClientButIsolatesRuns(t *testing.T) {
	ctx := hostContext(t, "alice")
	opened := make(chan superagent.ProviderConnection, 4)
	started := make(chan recordedModel, 4)
	first := scripted()
	second := scripted()
	factory := func(_ context.Context, c superagent.ProviderConnection) (superagent.ModelAdapterClient, error) {
		opened <- c
		return &factoryClient{
			generate: func(ctx context.Context, id string, req superagent.ModelRequest) (superagent.ModelStream, error) {
				started <- recordedModel{id, req, ctx}
				switch req.RunID {
				case "a":
					return first, nil
				case "b":
					return second, nil
				}
				return scripted(completed(c.Credential)), nil
			},
			close: func(context.Context) error { return nil },
		}, nil
	}
	var resolvers []string
	credential := "old"
	secrets := credentialFunc(func(_ context.Context, ref string) (string, error) {
		resolvers = append(resolvers, ref)
		return credential + "/" + ref, nil
	})
	cfg := twoModelConfig(factory)
	cfg.Providers["secondary"] = superagent.ProviderConfig{Adapter: "local-test", Endpoint: "https://other.example/v1", CredentialRef: "secret/secondary"}
	cfg.Models["other"] = superagent.ModelBindingConfig{Provider: "secondary", ModelID: "other-id"}
	b, err := superagent.NewModelBindings(ctx, cfg, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	if !reflect.DeepEqual(resolvers, []string{"secret/primary", "secret/secondary"}) {
		t.Fatalf("secret resolutions = %v", resolvers)
	}
	if p, s := recv(t, ctx, opened), recv(t, ctx, opened); p.Endpoint != "https://provider.example" ||
		p.Credential != "old/secret/primary" || s.Endpoint != "https://other.example/v1" ||
		s.Credential != "old/secret/secondary" || len(opened) != 0 || len(started) != 0 {
		t.Fatal("incorrect provider initialization or provider request at assembly")
	}
	delete(b.LocalModels(), "chat")
	client := clientForModels(t, b)
	a := start(t, ctx, client, request("a"))
	reqB := request("b")
	reqB.Model = "fast"
	other := start(t, ctx, client, reqB)
	calls := make(map[superagent.RunID]recordedModel)
	for range 2 {
		call := recv(t, ctx, started)
		calls[call.req.RunID] = call
	}
	if calls["a"].modelID != "chat-v1" || calls["b"].modelID != "fast-v2" {
		t.Fatalf("incorrect bindings: %+v", calls)
	}
	credential = "new"
	rotated, err := superagent.NewModelBindings(ctx, cfg, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer rotated.Close(ctx)
	if err := a.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, a, superagent.Cancelled)
	if calls["b"].ctx.Err() != nil {
		t.Fatal("cancellation reached another Run")
	}
	second.events <- completed("old-run-finished")
	if r := wait(t, ctx, other, superagent.Succeeded); r.Answer.Text != "old-run-finished" {
		t.Fatalf("old binding result = %+v", r)
	}
	newRun := start(t, ctx, clientForModels(t, rotated), request("new"))
	if r := wait(t, ctx, newRun, superagent.Succeeded); r.Answer.Text != "new/secret/primary" {
		t.Fatal("new Run did not use new client")
	}
}

func TestModelFactoryRollbackAndErrors(t *testing.T) {
	ctx := hostContext(t, "alice")
	const secret = "test-sensitive-value"
	for _, mode := range []string{"config", "credential", "client", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var closed, opened, lookedUp atomic.Int32
			setupCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			cfg := twoModelConfig(func(ctx context.Context, c superagent.ProviderConnection) (superagent.ModelAdapterClient, error) {
				opened.Add(1)
				if c.Endpoint == "https://second.example" {
					if mode == "cancel" {
						cancel()
						return nil, ctx.Err()
					}
					return nil, errors.New(secret)
				}
				return &factoryClient{close: func(ctx context.Context) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					closed.Add(1)
					return nil
				}}, nil
			})
			cfg.Providers["second"] = superagent.ProviderConfig{Adapter: "local-test", CredentialRef: "second", Endpoint: "https://second.example"}
			cfg.Models["bad"] = superagent.ModelBindingConfig{Provider: "second", ModelID: "second-model"}
			want := superagent.ModelClientInitFailed
			if mode == "config" {
				cfg.Providers["second"] = superagent.ProviderConfig{Adapter: "local-test", Endpoint: "https://" + secret + "@second.example"}
				want = superagent.ModelConfigInvalid
			} else if mode == "credential" {
				want = superagent.ModelCredentialUnavailable
			}
			b, err := superagent.NewModelBindings(setupCtx, cfg, credentialFunc(func(_ context.Context, ref string) (string, error) {
				lookedUp.Add(1)
				if mode == "credential" && ref == "second" {
					return "", errors.New(secret)
				}
				return secret, nil
			}))
			if b != nil || err == nil || strings.Contains(err.Error(), secret) {
				t.Fatal("assembly produced bindings or unredacted error")
			}
			if mode == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation error = %v", err)
				}
			} else {
				var setup *superagent.ModelSetupError
				if !errors.As(err, &setup) || setup.Code != want {
					t.Fatalf("setup error = %v, want %s", err, want)
				}
			}
			if mode == "config" {
				if lookedUp.Load() != 0 || opened.Load() != 0 || closed.Load() != 0 {
					t.Fatal("invalid config caused partial assembly")
				}
			} else if closed.Load() != 1 {
				t.Fatalf("rollback closed %d clients", closed.Load())
			}
		})
	}
}

type adapterErrorStream struct{ err error }

func (s adapterErrorStream) Recv() (superagent.ModelEvent, error) {
	return superagent.ModelEvent{}, s.err
}

func (adapterErrorStream) Close() error { return nil }

func TestModelFactoryNormalizesRequestFailuresAndClosesOnce(t *testing.T) {
	ctx := hostContext(t, "alice")
	for _, tc := range []struct {
		name     string
		fromRecv bool
		err      error
		code     superagent.StopCode
	}{
		{"authentication", false, superagent.ErrModelAuthFailed, superagent.StopModelAuthFailed},
		{"absent model", true, superagent.ErrModelNotFound, superagent.StopModelNotFound},
		{"generic connection", false, io.ErrUnexpectedEOF, superagent.StopModelFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opened, closed, requests atomic.Int32
			cfg := twoModelConfig(func(context.Context, superagent.ProviderConnection) (superagent.ModelAdapterClient, error) {
				opened.Add(1)
				return &factoryClient{
					generate: func(context.Context, string, superagent.ModelRequest) (superagent.ModelStream, error) {
						requests.Add(1)
						if tc.fromRecv {
							return adapterErrorStream{tc.err}, nil
						}
						return nil, tc.err
					},
					close: func(context.Context) error { closed.Add(1); return errors.New("unsanitized diagnostics") },
				}, nil
			})
			b, err := superagent.NewModelBindings(ctx, cfg, credentialFunc(func(context.Context, string) (string, error) { return "token", nil }))
			if err != nil {
				t.Fatal(err)
			}
			if opened.Load() != 1 || requests.Load() != 0 {
				t.Fatal("assembly generated a request")
			}
			run := start(t, ctx, clientForModels(t, b), request("run"))
			res := wait(t, ctx, run, superagent.Failed)
			if res.Stop.Code != tc.code || requests.Load() != 1 {
				t.Fatalf("stop = %+v, requests = %d", res.Stop, requests.Load())
			}
			var group sync.WaitGroup
			for range 4 {
				group.Add(1)
				go func() {
					defer group.Done()
					if err := b.Close(ctx); !errors.Is(err, superagent.ErrModelCloseFailed) {
						t.Errorf("close error = %v", err)
					}
				}()
			}
			group.Wait()
			if n := closed.Load(); n != 1 {
				t.Fatalf("close calls = %d", n)
			}
		})
	}
}
