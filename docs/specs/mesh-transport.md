# Mesh 传输与配置契约

阶段 A 设计稿。先实现进程内分派，远程传输只保留接口边界。本文定义逻辑协议。远程线协议、编码和端点配置等接入远程实现时再定。

## 传输接口

```go
type MeshTransport interface {
    Call(ctx context.Context, req MeshRequest) (MeshReply, error)
    OpenStream(ctx context.Context, req MeshRequest) (MeshStream, error)
}

type MeshStream interface {
    Recv() (MeshFrame, error)
    Close() error
}
```

Mesh 先解析组件绑定，再交给传输接口。进程内传输直接调用登记的 Go 实现，传递类型化消息。请求、响应和流事件按快照使用，提供方不得再改写已交付的数据。调用方身份来自受信上下文，截止时间和调用级取消通过 `ctx` 传递。

| 逻辑消息 | 字段与约束 |
| --- | --- |
| 请求 | `RequestID` 关联本次调用，目标由 Mesh 解析，操作决定输入及返回类型。按操作携带 `RunID`、`CallID` 等业务标识 |
| 普通响应 | `MeshReply` 与请求关联，携带操作的响应或可识别的组件错误。工具业务失败通过 `ToolResult` 返回。 [工具回执契约](tool-invoke.md#toolresult) |
| 流帧 | 属于已打开的请求流，按顺序交付事件。`Run` 事件的 `Seq` 仍由运行器分配，模型完成标记仍由模型适配器发出 |
| 传输错误 | Go `error` 表示调用未获得有效响应。交付状态区分确认未交给提供方和可能已交给提供方。`RequestID` 用于关联本次尝试，业务记录继续关联 `RunID` 或 `CallID` |

## 普通请求与流

| 类型 | 语义 |
| --- | --- |
| `Call` | 适用于 `Start`、`Wait`、`Cancel`、工具调用和环境控制。先校验请求和目标。调用结束时交付一个有效响应或一个错误。组件接手后的结局按其自身契约确定 |
| `OpenStream` | 用于运行观察及模型流。打开时返回一个流或一个错误。单个读取者按顺序 `Recv`，入参 `ctx` 覆盖流的生命周期。`Close` 可重复，并解除该流上阻塞的读取 |
| 观察流 | 缓冲有界，慢观察者的积压只影响该订阅。超限时先返回已缓冲前缀，再报告超限。关闭或中断后，宿主可单独调用 `Wait` |
| 模型流 | 保持增量和完成标记的顺序。缓冲积压时向上游施加背压，关闭或取消可解除读写阻塞。运行器校验 `ModelCompleted` 后才可派发工具。提前 EOF 或中断按模型失败或 Run 的停止原因处理 |

传输缓冲有字节上限，保留流的结束原因，意外中断返回可识别的错误。SDK 观察流正常交付 `run.finished` 后再返回 EOF。阶段 A 提供一次实时订阅，重放及断线续订随持久化评审。 [SDK 订阅与终态](go-sdk-contract.md#订阅与终态) · [模型断流规则](model-adapter.md#断流与异常结束)

## 取消和断连

请求上下文约束本次 `Call` 或流。已受理的 Run 由运行器按独立预算推进。取消整个 Run 要向运行器发出显式 `Cancel`。运行器记录取消意图后停止新派发，并向在途模型、工具和环境传递取消。环境清理使用独立的有界上下文。

| 场景 | 调用方看到的行为 |
| --- | --- |
| `Start` 可能已受理，响应丢失 | 返回受理结果不明。用原绑定和 `RunID` 查询已存在的记录 |
| `Observe` 中断或 `Wait` 超时 | 结束本次观察或等待，运行结果仍以运行器保存的终态为准 |
| `Cancel` 响应丢失 | 取消意图可能已记录。宿主可对原 Run 重送取消，终态由 `Wait` 确认 |
| 工具调用未取得响应 | Mesh 有可靠证据证明未交付时，可判为明确未执行。可能已交付且无有效回执时，运行器按结果不明处理 |
| 环境取消或释放的响应丢失 | 清理待确认，按原句柄查询。环境终止状态与工具效果各自确认 |
| 运行器进程退出 | 阶段 A 的内存记录随进程失效。跨重启恢复由 B 定义 |

阶段 A 通过取消上下文、关闭流和测试传输替身模拟断连或回包丢失。Mesh 对每次调用最多派发一次。允许重送的操作由调用方按相应契约发起新请求。 [工具调用生命周期](tool-call-lifecycle.md) · [执行环境清理](execution-environment.md#取消与清理)

## 本地配置格式

文件配置草案采用带版本的 YAML。键是逻辑引用，`local` 是宿主为 Go 实现登记的名称。宿主按类型解析名称，填入以逻辑引用为键的 `MeshConfig`。纯 Go 宿主可以直接构造同一份配置。

```yaml
version: 1
runners:
  main:
    models: [chat]
    tools: [search]
    default_environment: workspace
models:
  chat:
    local: chat-adapter
tools:
  search:
    local: bounded-search
environments:
  workspace:
    local: local-workspace
```

- `runners` 声明运行器可见的模型、工具和可选默认环境，宿主补充运行配置和授权策略。Run 受理时仍按宿主授权选择本次使用的工具。
- `models`、`tools`、`environments` 将逻辑引用映射到当前进程的本地实现。模型的供应商连接和凭据由宿主侧模型装配提供。 [模型工厂](model-factory.md)
- 装配时拒绝不支持的版本、未知字段、重复键、找不到的引用或不匹配的实现类型。通过验证的绑定在 Mesh 生命周期内固定。

## 阶段 A 验收

| 场景 | 验收观察 |
| --- | --- |
| 本地请求转发 | 同一 `Client` 通过 Mesh 找到运行器和提供方，保留类型、标识、取消和组件错误 |
| 取消请求回执丢失 | 重送 `Cancel` 可确认取消意图，已记录的取消只生效一次，`Wait` 返回唯一终态 |
| 派发前失败 / 执行成功后回执丢失 | 前者工具执行零次，报告明确未执行。后者工具执行一次，运行以 `ToolOutcomeUnknown` 收尾 |
| 流关闭、超限和提前 EOF | 关闭能解除阻塞，观察停止后 Run 可继续，模型完成标记缺失时本轮无法派发工具 |
| 校验配置 | 构造阶段发现重复键、未登记实现或错误类型。使用相同组件装配时，YAML 和 Go 配置解析出相同的本地绑定 |
