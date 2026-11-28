# 运行器与模型适配器契约

阶段 A 设计稿，更新于 2026-11-27。一次 `Generate` 对应一个 Run 的一个模型轮次 `Step`。运行器负责组装上下文、校验回复和派发工具。适配器负责请求模型，并把供应商响应转换成规范化事件。进程内和远程 Mesh 绑定遵守同一契约。

## 请求

```go
type ModelProvider interface {
    Generate(ctx context.Context, req ModelRequest) (ModelStream, error)
}

type ModelRequest struct {
    RunID          RunID
    Step           uint32
    Model          ModelRef
    Instruction    string
    Messages       []Message
    Tools          []ModelTool
    MaxOutputBytes uint64
}

type ModelTool struct {
    Name        string
    Description string
    InputSchema json.RawMessage
}
```

| 字段 | 约定 |
| --- | --- |
| `RunID`、`Step` | 运行器生成并关联本次请求。`Step` 从 1 开始，按模型请求递增 |
| `Model` | Run 受理时确定的逻辑模型引用。适配器使用绑定已选定的供应商模型和客户端。 [模型工厂配置](model-factory.md#配置与装配) |
| `Instruction`、`Messages` | 本轮的固定输入快照。消息按顺序包含 `StartRequest.History`、本次用户输入及此前已提交的完整轮次。适配器保留顺序和工具配对。 [组装与适配](message-assembly.md#运行器如何组装每轮) |
| `Tools` | 本次 Run 已授权且可展示给模型的工具定义。包含名称、描述和输入约束。空集合表示纯对话 |
| `MaxOutputBytes` | 从 Run 共享输出余额授予本轮的正数上限。按完整文本、工具标识、名称和参数的原始 UTF-8 字节计量。增量与最终快照中的相同内容只计一次 |

运行器在派发前检查期限、调用次数、输入体积和输出余额。适配器在读取、解码和组装时执行每帧、缓冲和本轮上限，运行器在接受增量和完整回复前复核本轮上限。Mesh 保留参与计量的字段内容和参数字节。 [Run 预算计量](run-budget.md#预算与扣减)

后续启用上下文压缩时，支持该能力的绑定提供独立的计量接口，并在 `Generate` 发送前核对计量指纹。 [压缩计量接口](context-compaction-contract.md#计量与发送)

## 流式事件与完整回复

```go
type ModelStream interface {
    Recv() (ModelEvent, error)
    Close() error
}

type ModelEventKind string

const (
    ModelTextDelta  ModelEventKind = "text.delta"
    ModelCompleted ModelEventKind = "completed"
)

type ModelEvent struct {
    Kind      ModelEventKind
    TextDelta string
    Reply     *ModelReply
}

type ModelReply struct {
    Finish    ModelFinishReason
    Text      string
    ToolCalls []ModelToolCall
    Usage     *Usage
}

type ModelFinishReason string

const (
    ModelFinishStop      ModelFinishReason = "stop"
    ModelFinishToolCalls ModelFinishReason = "tool_calls"
)

type ModelToolCall struct {
    ID        CallID
    Name      string
    Arguments json.RawMessage
}
```

- **`ModelTextDelta`** 只携带非空的用户可见文本。运行器按顺序转成 SDK 的 `text.delta`，供宿主显示草稿。
- **`ModelCompleted`** 携带一份自包含的 `Reply`。它包含完整文本、按模型声明顺序排列的工具调用、结束原因、可用的本轮用量。成功的流只交付一次完成事件，前面可以有零条或多条文本增量。
- **`Finish`** 为 `stop` 时，工具列表须为空，文本可以为空。为 `tool_calls` 时，须有至少一条工具调用，文本可选。适配器等待供应商的有效完成信号和完整内容块，再交付 `ModelCompleted`。其他无法映射的停止原因作为模型错误返回，并保留原因供诊断。
- **`CallID`** 在 Run 内唯一。适配器负责映射供应商标识，同一工具声明及其结果在后续模型请求中保持关联。参数采用完整的 JSON 对象，供应商增量在适配器内部组装。
- **用量**按逻辑模型请求报告一次。适配器归并供应商的累计用量，缺失时使用 `nil`。

`Recv` 每次返回一个事件或一个错误，`Kind` 决定有效载荷。单个调用方按顺序读取，`Generate` 的 `context` 覆盖整个流。`Close` 可重复调用，也可解除阻塞的 `Recv`。运行器收到完整 `ModelCompleted` 事件后即可校验并关闭流，后续关闭错误记入诊断。

宿主可用 `NewAnthropicMessagesAdapter(maxOutputTokens)` 登记 Anthropic Messages。适配器按 API 版本 `2023-06-01` 发送普通文本和客户端工具。在完整内容块、结束原因和 `message_stop` 齐全后将 `end_turn`、`tool_use` 映射为完成事件，`max_tokens` 映射为截断。端点使用带可选路径前缀的基础 URL，适配器追加 `/v1/messages`。 [API 版本](https://platform.claude.com/docs/en/api/messages/create?version=2023-06-01) · [流式事件](https://platform.claude.com/docs/en/build-with-claude/streaming#event-types)

有实测窗口及本地计量配置的宿主可使用 `NewMeteredAnthropicMessagesAdapter`，按供应商 `ModelID` 提供 `AnthropicInputProfile`。本地计量函数接收与 HTTP POST 相同的请求编码，适配器在发送前核对指纹。摘要独立放入历史消息。启用条件见 [上下文压缩验收门槛](stage-a-acceptance.md#上下文压缩接入门槛)。

兼容网关可用 `NewAnthropicMessagesAdapterWithOptions` 选择 Bearer 认证。仅对已验证可省略思考块的网关启用 `DiscardThinking`。思考块和签名独立限额，仍要求流完整结束。真实验收可通过 `SUPERAGENT_ANTHROPIC_BASE_URL`、`SUPERAGENT_ANTHROPIC_AUTH_MODE=bearer` 和 `SUPERAGENT_ANTHROPIC_DISCARD_THINKING=1` 配置相同的连接。

## 工具派发边界

1. 运行器完整收到 `ModelCompleted` 后，检查结束原因与工具列表是否一致，并检查整批调用的标识、名称和参数 JSON 对象。标识须在本 Run 内首次出现。缺失或重复标识、非法 JSON、结束原因冲突均视为模型协议错误，整批工具均未派发。
2. 运行器接受完整回复后发布 SDK 的 `message.completed`。`stop` 分支提交最终答复。`tool_calls` 分支按原顺序逐个检查取消、预算、工具授权和参数约束，然后经 Mesh 派发。
3. 合法 JSON 对象未通过工具约束，或工具名不在该 Run 的授权集合时，运行器记录 `tool.completed(rejected)` 并生成可关联的工具错误，用于后续模型输入。该调用的工具实现执行次数为零。
4. 本轮所有可回传的工具结果齐全且当前轮次可提交时，运行器一起提交模型消息和工具结果。随后按 Run 余额决定是否发起下一轮。已派发工具效果不明时，按阶段 A 的工具终态规则收尾。 [工具结果提交边界](message-assembly.md#工具结果何时进入上下文)

取消、截止时间、完整回复接收和工具派发由运行器按 Run 裁决。停止条件先于派发边界生效时，运行器关闭模型流，忽略迟到的模型事件，已发布的预览可保留。同轮已有工具派发时，外部效果仍按工具回执判定。

## 断流与异常结束

| 情况 | 运行器行为 |
| --- | --- |
| `Generate` 失败，或 `Recv` 在 `ModelCompleted` 前返回 `io.EOF`、网络错误或供应商错误 | Run 以 `Failed` 结束，保存可识别的模型失败原因。阶段 A 每次模型请求只尝试一次 |
| 供应商报告截断，或 `ModelCompleted` 的结构或结束原因不合法 | Run 以 `Failed` 结束，原因区分模型截断与协议错误 |
| 适配器或运行器确认模型输出超出字段、单帧或累计字节上限 | 立即关闭模型流。Run 以 `BudgetExceeded` 结束，保留可识别的上限原因，本轮工具派发次数为零 |
| Run 已取消或超过截止时间 | 由运行器记录 `Cancelled` 或 `TimedOut`，结束对应模型流。独立的供应商超时按模型失败处理 |
| 已收到并接受有效 `ModelCompleted` 后，模型连接关闭或清理出错 | 本轮继续使用完整回复，由运行器按终止或工具调用分支推进 |

完成事件前的文本仅作草稿，当前轮次缺少有效完成事件时，`Result.Messages` 只保留本次输入和此前已提交的轮次。模型流断开会由运行器裁决 Run 终态，宿主的 `Observe` 连接断开只结束观察。 [SDK 事件与结果](go-sdk-contract.md#事件和结果)

## 验收场景

阶段 A 通过 Mesh 调用进程内适配器，并注入流中断与提前 EOF。远程传输接入后可沿用同一组场景。

| 场景 | 可观察结果 |
| --- | --- |
| 多条增量后完整答复，或直接交付 `ModelCompleted(stop)` | 最终文本只显示一次。`Wait` 返回最终答复 |
| 供应商分片提供工具参数 | `ModelCompleted(tool_calls)` 之前派发次数为零。接受回复后按声明顺序执行，下一轮请求包含声明和对应结果 |
| 第二个工具标识与第一个重复，或其参数 JSON 不完整 | 整批工具派发次数为零。Run 报模型协议错误或截断 |
| 参数是完整 JSON 对象，但未通过工具约束 | 对应调用由运行器拒绝。可关联的错误随完整工具批次回传模型 |
| 出现文本后断流，或供应商以截断结束 | 界面保留草稿。该轮未产生 `message.completed`，工具派发次数为零 |
| 已接受完整回复后底层连接关闭 | 按回复推进，最终结果与正常关闭一致 |
| 收到 `ModelCompleted` 后、第一次工具派发前取消生效 | 待派发工具执行次数为零。Run 返回取消终态 |


真实供应商冒烟使用 `TestAnthropicLiveSearch`。宿主提供 `ANTHROPIC_API_KEY` 和 `SUPERAGENT_ANTHROPIC_MODEL`，并设置 `SUPERAGENT_ANTHROPIC_LIVE=1` 后运行 `go test ./tests/sdk -count=1 -run '^TestAnthropicLiveSearch$' -v`。普通测试用本地 HTTP 服务器覆盖协议和两个模型回合。
