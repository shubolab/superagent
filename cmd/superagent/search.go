package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/durable"
)

const scanLimit = 1 << 20

type searchAssembly struct {
	environment sdk.EnvironmentProvider
	requirement sdk.EnvironmentRequirement
	tool        sdk.LocalToolBinding
}

func configureSearch(ctx context.Context, workspace, state string, parallel int) (searchAssembly, error) {
	if strings.TrimSpace(workspace) == "" {
		return searchAssembly{}, errors.New("需要有效的 -workspace 目录")
	}
	absolute, err := filepath.Abs(workspace)
	if err != nil {
		return searchAssembly{}, fmt.Errorf("无效的 -workspace: %w", err)
	}
	root, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return searchAssembly{}, fmt.Errorf("无效的 -workspace: %w", err)
	}
	// The runner keeps the original state path as part of its session authority.
	physicalState, err := resolveStatePath(state)
	if err != nil {
		return searchAssembly{}, fmt.Errorf("无效的状态目录: %w", err)
	}
	receiptDir := filepath.Join(physicalState, "search-receipts")
	for _, dir := range []string{physicalState, receiptDir, filepath.Join(physicalState, "runs"), filepath.Join(physicalState, "sessions")} {
		resolved, err := resolveStatePath(dir)
		if err != nil {
			return searchAssembly{}, fmt.Errorf("无法核对状态目录: %w", err)
		}
		inside, err := directoriesOverlap(root, resolved)
		if err != nil {
			return searchAssembly{}, fmt.Errorf("无法核对工作区和状态目录: %w", err)
		}
		if inside {
			return searchAssembly{}, errors.New("工作区与状态目录不能重叠")
		}
	}
	const workspaceRef sdk.WorkspaceRef = "search-root"
	env, err := sdk.NewLocalReadEnvironmentWithOptions(map[sdk.WorkspaceRef]sdk.WorkspaceConfig{
		workspaceRef: {Root: root, ReadPrefixes: []string{"."}},
	}, sdk.LocalReadEnvironmentOptions{MaxScanBytes: scanLimit, MaxConcurrentReads: parallel})
	if err != nil {
		return searchAssembly{}, fmt.Errorf("无法装配只读工作区: %w", err)
	}
	identity, err := env.(sdk.RecoveryWorkspace).RecoveryWorkspace(ctx, sdk.RunScope{Workspace: workspaceRef})
	if err != nil {
		return searchAssembly{}, fmt.Errorf("无法核对工作区身份: %w", err)
	}
	// Keep the logical runner and its legacy text binding. Search pins the actual
	// directory in its own environment reference, which is part of the frozen request.
	key, err := durable.Digest([2]string{root, identity})
	if err != nil {
		return searchAssembly{}, err
	}
	requirement := sdk.EnvironmentRequirement{Ref: sdk.EnvironmentRef("search-" + key), Workspace: workspaceRef}
	binding := sdk.NewBoundedFileSearch()
	binding.Tool, err = sdk.NewLocalReceiptTool(receiptDir, binding.Tool)
	if err != nil {
		return searchAssembly{}, err
	}
	return searchAssembly{env, requirement, binding}, nil
}

func (s searchAssembly) bind(cfg *sdk.MeshConfig, runner *sdk.RunnerConfig, req *sdk.StartRequest) {
	cfg.LocalEnvironments = map[sdk.EnvironmentRef]sdk.EnvironmentProvider{s.requirement.Ref: s.environment}
	cfg.LocalTools = map[sdk.ToolRef]sdk.LocalToolBinding{"search": s.tool}
	runner.AllowedTools = map[sdk.SubjectID]map[sdk.ToolRef]sdk.ToolPermission{
		"terminal": {"search": {Environment: s.requirement, ReadPrefix: "."}},
	}
	runner.DefaultBudget.MaxToolRequests = pointer(uint64(2))
	runner.DefaultBudget.MaxToolScanBytes = pointer(uint64(scanLimit))
	runner.BudgetCaps = runner.DefaultBudget
	req.Tools, req.Environment = []sdk.ToolRef{"search"}, &s.requirement
}

// A new state directory may be missing. Resolve the deepest existing ancestor
// before checking overlap or creating anything under that path.
func resolveStatePath(path string) (string, error) {
	physical, err := filepath.EvalSymlinks(path)
	if !errors.Is(err, os.ErrNotExist) {
		return physical, err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return "", err
	}
	physical, err = resolveStatePath(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(physical, filepath.Base(path)), nil
}

// Check actual ancestor identities so symlink aliases and case-insensitive paths
// cannot let the search root include its own journal or session store.
func directoriesOverlap(workspace, state string) (bool, error) {
	work, err := os.Stat(workspace)
	if err != nil {
		return false, err
	}
	if !work.IsDir() {
		return false, fmt.Errorf("无效的 -workspace: %s 不是目录", workspace)
	}
	stateInfo, err := os.Stat(state)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if stateInfo != nil {
		if !stateInfo.IsDir() {
			return false, fmt.Errorf("%s 不是状态目录", state)
		}
		contained, err := hasAncestor(workspace, stateInfo)
		if contained || err != nil {
			return contained, err
		}
	}
	return hasAncestor(state, work)
}

func hasAncestor(path string, root os.FileInfo) (bool, error) {
	for {
		info, err := os.Stat(path)
		switch {
		case err == nil:
			if os.SameFile(info, root) {
				return true, nil
			}
		case !errors.Is(err, os.ErrNotExist):
			return false, err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return false, nil
		}
		path = parent
	}
}
