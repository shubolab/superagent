//go:build darwin || linux

package superagent_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/shubolab/superagent"
)

const yamlMesh = `version: 1
runners:
  main:
    models: [chat]
    tools: [search]
    default_environment: local
models:
  chat:
    local: chat-impl
tools:
  search:
    local: search-impl
environments:
  local:
    local: env-impl
`

func namedSearchCatalog(cfg superagent.MeshConfig) superagent.MeshConfig {
	return superagent.MeshConfig{
		LocalRunners:      cfg.LocalRunners,
		LocalModels:       map[superagent.ModelRef]superagent.ModelProvider{"chat-impl": cfg.LocalModels["chat"]},
		LocalTools:        map[superagent.ToolRef]superagent.LocalToolBinding{"search-impl": cfg.LocalTools["search"]},
		LocalEnvironments: map[superagent.EnvironmentRef]superagent.EnvironmentProvider{"env-impl": cfg.LocalEnvironments["local"]},
		ResolveCaller:     cfg.ResolveCaller,
	}
}

func TestMeshYAMLMatchesGoAssembly(t *testing.T) {
	ctx := hostContext(t, "alice")
	runSearch := func(f *searchFixture, cfg superagent.MeshConfig) superagent.Result {
		t.Helper()
		client := newClient(t, cfg)
		req := f.request()
		req.Environment.Ref = ""
		unauthorized := f.request()
		unauthorized.Environment.Ref = ""
		unauthorized.Environment.Workspace = "wrong"
		if _, err := client.Start(ctx, unauthorized); !errors.Is(err, superagent.ErrAccessDenied) {
			t.Fatalf("default let an unauthorized workspace through: %v", err)
		}
		run := start(t, ctx, client, req)
		f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"))
		f.second.events <- completed("ready")
		result := wait(t, ctx, run, superagent.Succeeded)
		if req.Environment.Ref != "" || f.env.opens.Load() != 1 || f.env.scanned.Load() != 13 {
			t.Fatalf("authorization or search changed: opens %d, bytes %d", f.env.opens.Load(), f.env.scanned.Load())
		}
		return result
	}
	direct := newSearchFixture(t)
	policy := direct.cfg.LocalRunners["main"]
	policy.DefaultEnvironment = "local"
	direct.cfg.LocalRunners["main"] = policy
	first := runSearch(direct, direct.cfg)

	file := newSearchFixture(t)
	cfg, err := superagent.LoadMeshConfig(strings.NewReader(yamlMesh), namedSearchCatalog(file.cfg))
	if err != nil {
		t.Fatal(err)
	}
	if second := runSearch(file, cfg); !reflect.DeepEqual(first, second) {
		t.Fatalf("Go result %+v differs from YAML result %+v", first, second)
	}
}

func TestMeshYAMLRejectsBadBindings(t *testing.T) {
	fixture := newSearchFixture(t)
	catalog := namedSearchCatalog(fixture.cfg)
	for name, yaml := range map[string]string{
		"unsupported":           strings.Replace(yamlMesh, "version: 1", "version: 2", 1),
		"top-level unknown":     yamlMesh + "flag: true\n",
		"runner unknown":        strings.Replace(yamlMesh, "    tools:", "    toolz:", 1),
		"component unknown":     strings.Replace(yamlMesh, "local: chat-impl", "remote: chat-impl", 1),
		"duplicate ref":         yamlMesh + "  local:\n    local: env-impl\n",
		"duplicate field":       strings.Replace(yamlMesh, "    local: chat-impl", "    local: chat-impl\n    local: chat-impl", 1),
		"repeat visibility":     strings.Replace(yamlMesh, "[chat]", "[chat, chat]", 1),
		"missing policy":        strings.Replace(yamlMesh, "  main:", "  other:", 1),
		"undeclared visibility": strings.Replace(yamlMesh, "[chat]", "[other]", 1),
		"out-of-scope policy":   strings.Replace(yamlMesh, "[search]", "[]", 1),
		"missing default":       strings.Replace(yamlMesh, "default_environment: local", "default_environment: other", 1),
		"wrong type":            strings.Replace(yamlMesh, "local: chat-impl", "local: search-impl", 1),
		"multi-document":        yamlMesh + "---\nversion: 1\n",
		"oversized":             yamlMesh + strings.Repeat(" ", 1<<20),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := superagent.LoadMeshConfig(strings.NewReader(yaml), catalog); !errors.Is(err, superagent.ErrInvalidConfiguration) {
				t.Fatalf("LoadMeshConfig error = %v", err)
			}
		})
	}
	if len(fixture.calls) != 0 || fixture.env.allocates.Load() != 0 {
		t.Fatal("bad configuration activated a component")
	}
}
