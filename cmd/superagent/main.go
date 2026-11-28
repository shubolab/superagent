package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/tui"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("superagent", flag.ContinueOnError)
	model := flags.String("model", os.Getenv("ANTHROPIC_MODEL"), "Anthropic model ID")
	state := flags.String("state-dir", "", "local state directory")
	parallel := flags.Int("parallel", 1, "concurrent session Runs (1 or 2)")
	sessionA := flags.String("session-a", "甲", "first session ID")
	sessionB := flags.String("session-b", "乙", "second session ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*parallel != 1 && *parallel != 2) || strings.TrimSpace(*model) == "" ||
		strings.TrimSpace(*sessionA) == "" || strings.TrimSpace(*sessionB) == "" || *sessionA == *sessionB {
		return errors.New("需要 Anthropic 模型 ID、两个不同的会话 ID 和 1 或 2 个并发名额")
	}
	directory := *state
	if directory == "" {
		configDir, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		directory = filepath.Join(configDir, "superagent")
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	credential := os.Getenv("ANTHROPIC_API_KEY")
	auth := sdk.AnthropicAuthAPIKey
	if token := os.Getenv("ANTHROPIC_AUTH_TOKEN"); token != "" {
		credential, auth = token, sdk.AnthropicAuthBearer
	}
	if credential == "" {
		return errors.New("需要 ANTHROPIC_API_KEY 或 ANTHROPIC_AUTH_TOKEN")
	}
	bindings, err := sdk.NewModelBindings(ctx, sdk.ModelFactoryConfig{
		Providers: map[sdk.ProviderRef]sdk.ProviderConfig{"anthropic": {
			Adapter: "messages", Endpoint: os.Getenv("ANTHROPIC_BASE_URL"), CredentialRef: "environment",
		}},
		Models: map[sdk.ModelRef]sdk.ModelBindingConfig{"chat": {Provider: "anthropic", ModelID: *model}},
		Adapters: map[string]sdk.ModelAdapter{"messages": sdk.NewAnthropicMessagesAdapterWithOptions(sdk.AnthropicMessagesOptions{
			Authentication: auth, MaxOutputTokens: 4096,
		})},
	}, environmentCredential(credential))
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bindings.Close(ctx)
	}()
	budget := sdk.Budget{
		Timeout: pointer(2 * time.Minute), MaxModelRequests: pointer(uint64(4)),
		MaxModelInputBytes: pointer(uint64(256 << 10)), MaxOutputBytes: pointer(uint64(64 << 10)),
	}
	mesh, err := sdk.NewMesh(sdk.MeshConfig{
		LocalRunners: map[sdk.RunnerRef]sdk.RunnerConfig{"local": {
			AllowedModels: map[sdk.SubjectID][]sdk.ModelRef{"terminal": {"chat"}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 256 << 10, ResultRetention: 5 * time.Minute,
			Recovery:   &sdk.RecoveryConfig{Directory: filepath.Join(directory, "runs"), BindingVersion: "terminal-v1", ReconcileTimeout: time.Second},
			Scheduling: &sdk.SessionSchedulingConfig{MaxActiveRuns: *parallel, MaxQueuedRuns: 2, MaxQueueWait: 30 * time.Second},
		}},
		LocalModels:   bindings.LocalModels(),
		ResolveCaller: func(context.Context) (sdk.SubjectID, error) { return "terminal", nil },
	})
	if err != nil {
		return err
	}
	client, err := sdk.NewClient(mesh, "local")
	if err != nil {
		return err
	}
	return tui.Run(ctx, os.Stdin, os.Stdout, tui.Config{
		Client: client, Sessions: [2]sdk.SessionID{sdk.SessionID(*sessionA), sdk.SessionID(*sessionB)},
		Store:   sdk.SessionConfig{Directory: filepath.Join(directory, "sessions"), MaxTurns: 512, MaxHistoryBytes: 128 << 10},
		Request: sdk.StartRequest{Model: "chat"},
	})
}

type environmentCredential string

func (c environmentCredential) Resolve(context.Context, string) (string, error) {
	return string(c), nil
}

func pointer[T any](value T) *T { return &value }
