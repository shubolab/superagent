package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	EnvironmentRef              = legacy.EnvironmentRef
	WorkspaceRef                = legacy.WorkspaceRef
	EnvironmentHandle           = legacy.EnvironmentHandle
	EnvironmentGrantRef         = legacy.EnvironmentGrantRef
	EnvironmentRequirement      = legacy.EnvironmentRequirement
	RunScope                    = legacy.RunScope
	CallScope                   = legacy.CallScope
	EnvironmentProvider         = legacy.EnvironmentProvider
	CapabilityEnforcement       = legacy.CapabilityEnforcement
	EnvironmentReadCapabilities = legacy.EnvironmentReadCapabilities
	EnvironmentReadDescriptor   = legacy.EnvironmentReadDescriptor
)

const (
	CapabilityEnforced    = legacy.CapabilityEnforced
	CapabilityMeasured    = legacy.CapabilityMeasured
	CapabilityUnavailable = legacy.CapabilityUnavailable
)

var (
	ErrEnvironmentBusy = legacy.ErrEnvironmentBusy
	ErrReadLimit       = legacy.ErrReadLimit
)
