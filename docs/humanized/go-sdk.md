# Go SDK 图解

阶段 A 评审稿。Go 宿主和 TUI 使用同一套接口，经 Mesh 调用进程内运行器。 [宿主装配图解](go-sdk-assembly.md)

## 一次 Run 的生命周期

```mermaid
flowchart TB
    start["Start(RunID)<br/>等待运行器受理"] --> run["Run · 由运行器持有"]
    run --> observe["Observe<br/>接收实时事件"]
    run --> wait["Wait<br/>读取最终结果"]
    cancel["Cancel<br/>请求显式停止"] --> run

    classDef owner fill:#166071,color:#fff,stroke:#166071;
    class run owner;
```

## Agent Loop

运行器从受理开始独立推进 Agent Loop。`Start` 返回 Run 句柄，宿主可以观察、等待或取消。一次 Run 可以往返调用模型和工具多轮。

```mermaid
---
config:
  flowchart:
    nodeSpacing: 26
    rankSpacing: 32
---
flowchart TB
    admitted["Run 已受理"] --> check{"可以继续？"}
    check -->|取消、超时或超预算| stopped["记录原因并结束"]
    check -->|可以| model["请求模型 · 流式输出"]
    model --> reply{"模型回复完整有效？"}
    reply -->|中断或无效| stopped
    reply -->|完整且无工具| done["保存答复并结束"]
    reply -->|完整且有工具| tools["逐个校验<br/>经 Mesh 派发"]
    tools -->|停止或效果不明| stopped
    tools -->|结果齐全| context["把消息和结果加入上下文"]
    context --> check

    classDef active fill:#166071,color:#fff,stroke:#166071;
    class model,tools,context active;
```

可回传的工具失败或拒绝也计入本轮结果，模型可据此调整。每次派发前都会复查取消和预算。在途工具随 Run 接收取消信号，效果不明时按结果不明收尾。 [阶段 A 推进规则](../specs/stage-a-bypass.md#推进规则) · [Run 预算图解](run-budget.md)

模型流显式交付完整回复。此前的增量供界面显示，运行器接受完整回复后才处理拟调用的工具。 [模型流契约](../specs/model-adapter.md) · [上下文与消息组装图解](message-assembly.md)

## 三项取舍

| 建议 | 调用方看到的行为 |
| --- | --- |
| Run 独立于连接 | 启动受理后由运行器推进。关掉 TUI 视图或断开观察，Run 继续执行。`Cancel` 确认请求，终态仍从 `Wait` 取得 |
| 调用方预先生成 `RunID` | 启动结果不明时可用原 ID 查询或取消。同一运行器实例在记录保留期内对相同请求去重 |
| 实时事件有界 | `Observe` 供界面展示，慢消费时可能断流。`Wait` 返回权威终态，宿主用结果里的完整消息增量更新历史 |

## 事件示例 · 先用工具，再回复

```mermaid
sequenceDiagram
    participant R as 运行器
    participant U as Go 宿主 / TUI
    R-->>U: run.started
    R-->>U: text.delta（逐段显示）
    R-->>U: message.completed（拟调用工具）
    R-->>U: tool.dispatched（已派发）
    opt 工具调用在途
        R-->>U: tool.progress（已等待时长）
    end
    R-->>U: tool.completed（工具结局）
    R-->>U: text.delta（下一轮文本）
    R-->>U: message.completed（完整回复）
    R-->>U: run.finished
    U->>R: Wait
    R-->>U: Result
```

- **完整回复替换草稿。** 参考 Claude Code 的增量和完整内容边界，界面以每轮完整消息收束流式文本。 [Claude Code 对标表](../specs/go-sdk-contract.md#claude-code-对标)
- **拟调用、已派发、已有结果各自可见。** 同一次工具调用用 `CallID` 串起来。派发后效果不明时，Run 以结果不明结束。
- **查看最终状态用 `Wait`。** 观察流断开后，宿主仍可读取原 Run 的结果。

模型和工具通过可替换接口接入。工具按次获得执行环境授权。 [权限门禁图解](run-authorization.md) · [执行环境图解](execution-environment.md)

[详细契约与 Go 接口签名](../specs/go-sdk-contract.md) · [架构图解](architecture.md)
