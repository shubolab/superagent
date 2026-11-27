# Go SDK 契约与行为接口

状态为待审提案，更新于 2026-11-27。本文定义阶段 A 对 SDK 使用者和组件扩展方可见的行为。接口名称、类型字段及语义均供评审。参见 [SDK 图解](../humanized/go-sdk.md)、[总体架构](architecture.md) 和 [阶段 A · bypass](stage-a-bypass.md)。

## 宿主接口

宿主和 TUI 都使用 `Client` 访问通过 Mesh 绑定的运行器。SDK 提供具体的 `Client` 和 `Run` 类型。流和宿主扩展以小接口表达，本地和远程实现遵守同一契约。 [创建 Client 与组件登记](go-sdk-assembly.md)

一次 Run 可以包含多轮模型请求、工具派发和结果回传。循环由运行器推进，宿主通过 Run 句柄观察、等待或取消。 [Agent Loop 图解](../humanized/go-sdk.md#agent-loop) · [推进规则](stage-a-bypass.md#推进规则)

```go
type RunID string

type StartRequest struct {
    ID          RunID
    Instruction string
    History     []Message
    Input       UserMessage
    Model       ModelRef
    Tools       []ToolRef
    Environment *EnvironmentRequirement
    Budget      Budget
}

// Start 等待运行器受理，然后返回可独立观察、等待或取消的 Run。
func (c *Client) Start(ctx context.Context, req StartRequest) (*Run, error)

// Run 仅构造绑定到同一运行器的句柄。
func (c *Client) Run(id RunID) *Run

func (r *Run) ID() RunID
func (r *Run) Observe(ctx context.Context) (EventStream, error)
func (r *Run) Wait(ctx context.Context) (Result, error)
func (r *Run) Cancel(ctx context.Context) error

type EventStream interface {
    Recv() (Event, error)
    Close() error
}
```

`ModelRef` 和 `ToolRef` 是 Mesh 配置中的能力引用。`Tools` 从宿主已授权的范围中选择本 Run 的工具，空集合表示纯对话。`Environment` 描述本次运行所需的工作区或隔离能力，由运行器获取环境并按次授予工具访问范围。`Budget` 采用受理时固定的有效额度，运行器按需分配和扣减。运行器受理时固定输入与已授权的能力绑定。 [Run 权限门禁](run-authorization.md) · [Run 预算契约](run-budget.md)

`History` 是本次输入之前的快照，`Input` 与 `Instruction` 分别传入。 [消息结构与历史校验](message-assembly.md#宿主如何传历史)

## 启动与生命周期

| 行为 | 调用方可依赖的保证 |
| --- | --- |
| `Start` 成功 | 返回时运行器已受理，Run 可以已经开始或结束。每个 Run 至多提交一个终态 |
| 启动明确被拒绝 | 参数、预算、授权或所需绑定不合法时，返回明确的调用错误。该请求未创建 Run，也未调用模型或工具 |
| 启动回执丢失 | 返回可识别的「受理结果不明」。宿主保留原 `RunID`，可用 `Client.Run(id)` 向同一运行器等待结果或申请取消。此时查不到记录可能是受理尚未完成或记录已经丢失 |
| 重送相同启动请求 | 建议由调用方为每次逻辑启动生成一个非空 `RunID`。在同一调用方、同一运行器实例及记录保留期内，原 ID 和原请求返回同一 Run，沿用首次受理的绑定。并发重送也只受理一次。原 ID 配不同请求返回冲突 |
| `Wait` | 阻塞到终态可取，也可以提前超时。允许多个等待者，重复读取在结果保留期内得到相同快照 |
| `Cancel` | 确认运行器已记录取消意图，或 Run 已经结束。可重复调用。已接受的取消阻止新步骤，已派发的工作按依赖的能力停止。取消回执丢失时可对原 Run 重送。最终结果由 `Wait` 取得 |

阶段 A 的运行记录存在拥有 Run 的运行器实例中。运行中始终保留记录，结束后按运行器公布的窗口保留结果及启动去重记录。每次 `Start` 调用只提交一次启动请求，由宿主在能确认实例和保留窗口时选择是否重送。运行器重启、绑定改指新实例或记录过期后，原操作的外部效果可能仍然存在，SDK 返回相应的查询错误，宿主可结合业务记录处理。跨重启去重和恢复留到 B。

调用参数中的 `context.Context` 按操作划分。`Start` 的上下文控制受理调用，运行结束由 Run 的预算、显式取消和运行器生命周期决定。`Wait` 与 `Cancel` 的上下文分别约束本次等待和取消请求的确认。`Observe` 的上下文覆盖订阅及后续 `Recv`，关闭视图或断开订阅只停止观察。运行器再把 Run 的截止时间和取消信号经 Mesh 传递给依赖。 [Go 对逐调用 context 的说明](https://go.dev/blog/context-and-structs)

## 事件和结果

```go
type CallID string

type Event struct {
    RunID     RunID
    Seq       uint64
    Step      uint32
    Kind      EventKind
    TextDelta string
    Message   *AssistantPreview
    Tool      *ToolEvent
    Outcome   RunOutcome
}

type ToolCallRef struct {
    ID   CallID
    Name string
}

type AssistantPreview struct {
    Text      string
    ToolCalls []ToolCallRef
}

type ToolEvent struct {
    Call    ToolCallRef
    Elapsed time.Duration
    Outcome ToolOutcome
    Code    string
}

type Result struct {
    RunID    RunID
    Outcome  RunOutcome
    Answer   *AssistantMessage
    Messages []Message
    Usage    *Usage
    Stop     *StopDetail
}
```

### 事件类型与顺序

| `Kind` | 触发时机 | 有效载荷 |
| --- | --- | --- |
| `run.started` | 运行器完成受理，准备执行。它是逻辑事件流中的第一条 | `RunID`、`Seq` |
| `text.delta` | 模型生成用户可见的文本 | `Step`、`TextDelta`。同一步内按顺序追加 |
| `message.completed` | 运行器校验并接受本轮 `ModelCompleted` 后，准备提交答复或检查该轮工具调用。工具轮次的消息提交另按组装契约裁决 | `Step`、`Message`。完整展示文本和拟调用工具标识。界面用它替换同一步的预览 |
| `tool.dispatched` | 运行器校验名称、参数、授权和预算后跨过派发边界 | `Step`、`Tool.Call`。表示已发起调用。远端执行和外部效果仍以回执为准 |
| `tool.progress` | 工具调用在途时可选发出 | `Step`、`Tool.Call`、已等待时长 `Elapsed`。由运行器计时，反映等待时长 |
| `tool.completed` | 工具调用已有可报告结局，或校验明确拒绝调用 | `Step`、`Tool.Call`、`Outcome`、可选 `Code` |
| `run.finished` | 运行器提交 `Result` 后发出 | `RunID`、`Seq`、`Outcome`。它是该 Run 的最后一条逻辑事件 |

- 所有事件带 `RunID` 和 `Seq`。序号由拥有 Run 的运行器从 1 连续分配。只在同一 Run 内排序。允许在分配序号前合并相邻文本增量，收到的完整前缀连续。
- `Step` 从 1 开始，每次模型请求递增，同一轮产生的工具事件沿用该值。运行级事件的 `Step` 为 0。工具调用用 Run 内唯一的 `CallID` 关联，供应商的调用标识由适配器映射。
- 完整模型回复中的工具仅为拟调用。模型端中途断流或参数尚未完整时，已有文本可以作为草稿显示，运行器在完整回复通过验证后才开始派发。公共事件使用上表的标准化投影。后续模型所需的标准工具调用与结果由 `Result.Messages` 提供。
- 阶段 A 同一 Run 内按声明顺序派发工具，两个 Run 可以并行。每个已派发调用至多发出一个 `tool.completed`。前置校验失败可直接发出 `tool.completed(rejected)`；预算耗尽或取消后尚未处理的拟调用由界面随 `run.finished` 收束为未执行。

| `Tool.Outcome` | 判定与下一步 |
| --- | --- |
| `succeeded` | 工具返回成功。把对应结果交给模型 |
| `rejected` | 派发前的授权、参数或环境授权检查拒绝调用。可关联的拒绝原因作为工具错误交给模型 |
| `failed` | 已有明确失败，或能在派发后确认未执行，例如工具提供方拒绝授权。可关联的错误作为工具结果交给模型。系统故障按 Run 的失败语义处理 |
| `cancelled` | 在途调用已确认停止且效果可判定。运行器按已经生效的 Run 停止原因收尾 |
| `unknown` | 已派发或可能已派发，外部效果仍不明。运行器发出 `tool.completed(unknown)`，以 `ToolOutcomeUnknown` 提交终态 |

```text
run.started
  1  text.delta* -> message.completed [拟调用 CallID=x]
     tool.dispatched(x) -> tool.progress(x)* -> tool.completed(x, succeeded)
  2  text.delta* -> message.completed [最终答复]
run.finished(Succeeded)
```

### 订阅与终态

- `Observe` 建议每个 Run 只成功建立一次订阅。运行器从受理开始保留有界缓冲，快速完成的 Run 也可在缓冲和保留期内首次观察。一次订阅由一个读取者调用 `Recv`。
- `Recv` 交付 `run.finished` 后返回 `io.EOF`。缓冲溢出先交付已缓冲前缀，再返回可区分的溢出错误。收到终态之前传输中断返回流错误。`Close` 可重复调用，关闭订阅或取消其上下文会解除阻塞中的 `Recv`。`Close` 后返回流已关闭错误，上下文停止则返回上下文错误。
- 关闭、溢出和传输中断只结束这条观察流，宿主仍可用 `Wait` 读取结果。阶段 A 的观察流用完即止，重放及断线续订随持久化评审。流错误通过 `Recv` 返回。
- `Result.Messages` 是相对 `StartRequest.History` 的追加增量，包含本次 `Input` 一次和已提交的完整轮次。宿主按终态决定是否追加历史。`Answer` 是成功时最终助手消息的便捷视图，`Usage` 汇总能确认的用量，缺失表示不可用。 [返回的消息增量](message-assembly.md#run-结束后交回宿主)
- `run.finished` 和 `Wait` 来自同一终态。缓冲溢出或断连时观察者可能收不到终态事件，`Wait` 的结果仍以运行器保存的终态为准。调用方不得把文本结束或流关闭用作成功依据。

### 调用错误与运行终态

`Wait` 返回的 Go `error` 表示本次调用未取到终态，例如等待截止、连接中断，或运行记录已失效。只要终态可取，`Wait` 返回 `Result` 和 `nil`，由 `Result` 表达运行结果。

调用错误至少应能区分参数无效、拒绝访问、记录不可查、启动请求冲突、受理结果不明、依赖或传输不可达，以及流缓冲溢出。SDK 将本地与远程错误转换为同一组可判断的行为。错误类型的具体表示随接口一起评审。

| 终态 | 含义 |
| --- | --- |
| `Succeeded` | 模型完成最终答复 |
| `Cancelled` | 已按显式取消停止，所有已派发工具的结果可判定 |
| `TimedOut` | Run 达到截止时间，所有已派发工具的结果可判定 |
| `BudgetExceeded` | 剩余额度不足以准入下一步，或命中需要停止 Run 的字节上限。命中范围和限额见 `Stop` |
| `Failed` | 模型协议、依赖或执行失败，且不存在未确认的工具结果 |
| `ToolOutcomeUnknown` | 一个已派发或可能已派发的工具无法确认结果。记录调用 ID 及掉线、超时或取消等原因，停止后续工具派发 |

工具明确返回的业务错误可以交还模型，由模型在剩余预算内修正。`ToolOutcomeUnknown` 由运行器裁决。TUI 到运行器的连接中断只构成 SDK 调用错误，调用方可继续查询原 Run 的终态。多个停止事件竞争时，由运行器按 Run 裁决，已提交的终态保持固定。

取消由运行器在派发边界裁决。先受理取消就停止新的模型请求和工具派发。工具已越过派发边界时，取消信号传给在途工作，并以可验证的结果确定该调用的结局。若结束时仍无法确认工具效果，`ToolOutcomeUnknown` 优先于 `Cancelled`、`TimedOut` 或 `BudgetExceeded`，`Stop` 保留触发停止的原因。

## Claude Code 对标

| 对标能力 | Claude Code 公开行为 | SuperAgent 阶段 A 取舍 |
| --- | --- | --- |
| 流式输出与完整内容 | 开启部分消息后，CLI 和 Agent SDK 给出流式增量。Agent SDK 在内容块完成时也给出完整内容 | `text.delta` 加按模型轮次发布的 `message.completed`。TUI 用完整内容收束草稿。 [Agent SDK 流式文档](https://code.claude.com/docs/en/agent-sdk/streaming-output#message-flow) |
| 工具过程 | 工具参数块可流式生成。执行中的 `tool_progress` 带调用标识和已等待时间 | 区分拟调用、派发、等待和结果。`CallID` 贯穿工具事件，阶段 A 仅报告标准化的工具进度。 [工具流](https://code.claude.com/docs/en/agent-sdk/streaming-output#stream-tool-calls) · [工具进度](https://code.claude.com/docs/en/agent-sdk/typescript#sdktoolprogressmessage) |
| 运行结果 | `result` 报告运行结局、用量和可用答复。同一 CLI 会话流中后续仍可能有任务通知 | SuperAgent 按 Run 单独开流，`run.finished` 封闭本 Run。`Wait` 支持独立读取和重复读取终态。 [CLI 流式输出](https://code.claude.com/docs/en/headless#stream-responses) · [Agent SDK 结果定义](https://code.claude.com/docs/en/agent-sdk/typescript#sdkresultmessage) |
| 中断与后续输入 | Agent SDK 流式输入模式有 `interrupt()`，回执可以告知当时仍在队列中的消息 | 首版 `Cancel` 只作用于指定 Run。运行中插话、排队和接管在后续会话设计评审。 [中断回执](https://code.claude.com/docs/en/agent-sdk/typescript#sdkcontrolinterruptresponse) |

## 宿主扩展接口

Go SDK 可本地注册模型和工具实现，远程服务经 Mesh 发布同一能力。`ModelProvider`、`ModelStream` 和显式完整回复见 [运行器与模型适配器契约](model-adapter.md)。

| 扩展点 | 行为约束 |
| --- | --- |
| `Tool` | 工具注册声明名称和参数约束。`Invoke` 接收按次授权引用，回执区分已知结果、明确未执行和结果不明。 [Tool.Invoke 契约](tool-invoke.md) |
| 执行环境 | Run 可以声明环境要求。运行器持有环境句柄，工具凭本次调用授权使用环境。 [分配、资源上限与清理](execution-environment.md) |

`Client` 和 `Run` 句柄可被多个 goroutine 同时使用。共享的模型与工具实现应支持独立 Run 的并发调用，或各自按资源限流。不同调用方获得的数据快照可以独立使用。单次 Run 内仍由一个运行器推进，阶段 A 建议同轮工具按声明顺序执行。

## 第一轮评审

1. **运行是否独立于启动连接。** 建议受理后的 Run 独立运行，由预算和显式取消控制。这适用于切换 TUI 会话和 SDK 远程调用。宿主按需使用 `Cancel`，例如退出前取消其拥有的运行。
2. **启动回执不明时如何拿回句柄。** 建议 `RunID` 由调用方预先生成。相比由服务端返回 ID，调用方多准备一个字段，但首个响应丢失时仍能定位原请求。
3. **事件和终态如何消费。** 建议先确认增量和完整消息、工具派发和结果、单 Run 终态三组边界。阶段 A 采用有界实时流和独立 `Wait`，重放及断线续订随后评审。

阶段 A 接口验收使用 Go 宿主调用进程内运行器，并通过测试传输替身注入故障。 [Mesh 传输验收](mesh-transport.md#阶段-a-验收)

| 场景 | 验收观察 |
| --- | --- |
| 纯文本快速完成 | 首次订阅收到 `run.started`、完整消息、`run.finished` 和 EOF。增量与完整消息组合时文字只呈现一次 |
| 先查库存再回复 | 整批模型工具声明完成后才派发。工具事件的 `CallID` 一致。结果回传后第二步开始，事件 `Seq` 连续 |
| 工具参数约束不符或模型流截断 | 可关联的校验拒绝有 `tool.completed(rejected)`，对应工具执行次数为零。截断回复只有草稿和运行终态 |
| 慢工具和两路运行 | 甲停在工具时乙持续流式输出。启用工具进度时，甲的进度可归属到对应调用。取消甲或关掉甲的观察者，乙继续 |
| 观察断连或缓冲溢出 | `Recv` 返回对应流错误。Run 继续，`Wait` 返回原 Run 的终态，多个等待者拿到一致结果 |
| 派发后断连或取消竞争 | 无法确认工具效果时，工具和 Run 均报告结果不明。同轮后续工具停止派发。已提交终态保持固定 |
| 启动回执丢失 | 复用原 ID 可查询。保留窗口内同请求重送产生一次运行，不同请求使用同 ID 返回冲突 |
