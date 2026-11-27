# Tool.Invoke 契约

阶段 A 设计稿，更新于 2026-11-27。每次 `Invoke` 对应一个已校验的工具调用。进程内与远程绑定使用同一套请求和回执语义。 [工具生命周期](tool-call-lifecycle.md) · [执行环境契约](execution-environment.md)

## ToolRequest

```go
type Tool interface {
    Invoke(ctx context.Context, req ToolRequest) (ToolResult, error)
}

type EnvironmentGrantRef string

type ToolRequest struct {
    RunID            RunID
    CallID           CallID
    Tool             ToolRef
    Arguments        json.RawMessage
    EnvironmentGrant EnvironmentGrantRef
    Limits           ToolLimits
}
```

| 字段 | 契约 |
| --- | --- |
| `RunID`、`CallID` | 运行器使用本次 Run 和模型拟调用的规范标识。原请求、回执和恢复查询沿用这组标识 |
| `Tool`、`Arguments` | `Tool` 是 Run 授权范围内的工具绑定，参数是运行器校验过的 JSON 对象。接收方依据受信调用上下文复核实际绑定、关联标识、参数范围及业务权限。 [按次复核](run-authorization.md#按次授权怎么复核) |
| `EnvironmentGrant` | 由执行环境签发的按次授权引用，绑定 `RunID`、`CallID`、目标工具身份和受限环境。环境按获授路径、操作、额度及有效期核验实际访问。空值适用于无需执行环境的工具 |
| `Limits` | 运行器从 Run 余额授予的实际限额，包括 `MaxResultBytes` 和适用的 `MaxScanBytes`、`MaxRawOutputBytes`。工具提供方和执行环境各自执行对应上限。 [预算分配](run-budget.md#怎么分给模型和工具) |

运行器持有 Run 级环境句柄。工具通过 `EnvironmentGrant` 经 Mesh 请求环境操作。环境以已认证的工具身份和调用关联信息核验授权，授权在调用收尾或期限到达后失效。环境需求、授权或有效额度无法满足时，运行器在派发前停止该调用。 [绑定与按次授权](execution-environment.md#绑定与按次授权)

`ctx` 传递本次执行的取消和截止时间。远程适配器把它们经 Mesh 传给执行方。环境采用调用截止时间与授权到期时间中较早者执行。额度字段的精确形状和 Mesh 编码后续评审。

## ToolResult

```go
type ToolExecutionState string

const (
    ToolExecutionKnown       ToolExecutionState = "known"
    ToolExecutionNotExecuted ToolExecutionState = "not_executed"
    ToolExecutionUnknown     ToolExecutionState = "unknown"
)

type ToolResult struct {
    RunID       RunID
    CallID      CallID
    State       ToolExecutionState
    Completion *ToolCompletion
    Reason      *ToolReason
    Usage       *ToolUsage
}

type ToolCompletionOutcome string

const (
    ToolSucceeded ToolCompletionOutcome = "succeeded"
    ToolFailed    ToolCompletionOutcome = "failed"
    ToolCancelled ToolCompletionOutcome = "cancelled"
)

type ToolCompletion struct {
    Outcome   ToolCompletionOutcome
    Content   string
    Artifacts []ArtifactRef
}

type ToolReason struct {
    Code    string
    Message string
}

type ToolUsage struct {
    ScannedBytes   *uint64
    RawOutputBytes *uint64
}
```

| `State` | 有效字段与判定 |
| --- | --- |
| `known` | 必须有 `Completion`，其结局为成功、失败或已取消。工具提供方已经确认执行结果及相关效果。失败和取消必须带 `Reason`。已知失败仍可能包含已发生的效果 |
| `not_executed` | 必须有 `Reason`，`Completion` 为空。证据能够确认这次工具操作尚未开始，例如接收方在调用实现前拒绝授权 |
| `unknown` | 必须有 `Reason`，`Completion` 为空。工具操作可能已开始，尚无充分证据确认效果 |

`Content` 是有界的模型可见结果，完整大产物通过 `ArtifactRef` 返回。`Reason.Code` 供程序判断，`Reason.Message` 是可披露的简短说明。`Usage` 报告受管通道的实际字节，含按限额停止前的消耗。已确认的零用量显式为零，缺失表示不可确认或不适用。运行器按 `RunID`、`CallID` 和原工具绑定校验回执。 [大输出与结果大小](tool-call-lifecycle.md#大输出与结果大小)

### Go error 与回执

- **`err == nil`** 表示返回了一份可验证的 `ToolResult`。业务失败、明确未执行、工具提供方报告结果不明都通过回执表达。
- **`err != nil`** 表示本次调用没有拿到有效回执，返回的 `ToolResult` 为空。包括传输失败、等待截止和回执校验失败。运行器只有取得可靠的未执行证据时，才归为 `not_executed`。请求可能已到达时归为 `unknown`。
- **阶段 A** 使用已知业务结果推进循环。派发前的工具校验拒绝发布 `tool.completed(rejected)`。派发后确认未执行时发布 `tool.completed(failed)`，并带明确原因。结果不明则停止后续调用，以 `ToolOutcomeUnknown` 收尾。 [SDK 事件语义](go-sdk-contract.md#事件类型与顺序)
- **阶段 B** 对可恢复 Run 按原调用查询回执或关联证据。调用失败本身不触发重发 `Invoke`。 [回执恢复规则](tool-call-lifecycle.md#查原调用按证据接续)

## 取消与清理状态

| 状态 | 归属与含义 |
| --- | --- |
| 取消请求 | 运行器裁决 Run 的取消并停止新派发。工具提供方和环境确认收到请求后，发出相应的停止信号 |
| 工具执行结果 | 工具提供方确认操作结局。运行器根据可信回执和派发证据记录 `ToolResult` 对应的结局 |
| 资源清理 | 环境确认工作已停止和资源已释放。清理状态可为请求已受理、待确认或已释放，经独立的清理请求或查询取得 |

`Invoke` 的等待可因 `ctx` 结束而返回。收到取消确认或环境已释放，都无法单独证明文件是否已写入。`Wait` 读取 Run 终态，清理继续用独立的有界期限完成。 [环境的取消与清理](execution-environment.md#取消与清理)

## 验收场景

| 场景 | 预期 |
| --- | --- |
| 两次调用共享工作区，第二次尝试使用第一次的授权引用 | 环境依据 `CallID` 与目标工具校验并拒绝。获授路径和操作各自独立 |
| 接收方在工具操作开始前明确拒绝 | 收到 `not_executed` 和原因。工具操作次数为零 |
| 等待截止，Mesh 能确认请求未发出 | `Invoke` 返回等待错误。运行器依据传输证据判为 `not_executed` |
| 工具明确返回业务失败 | 收到 `known` 及 `failed`，运行器可将有界结果交给模型 |
| 文件保存回执已经确认，取消随后生效，Run 终态尚未提交 | 工具记录 `known` 及 `succeeded`。运行器按取消规则提交 Run 终态。环境另行确认清理 |
| 文件已保存但回执丢失，环境随后确认已释放 | A 报 `ToolOutcomeUnknown`，清理显示已释放。B 在接续前查询原调用证据 |
