# SuperAgent 架构

设计评审稿。独立 TUI 和 Go SDK 共用 Go 执行内核。

## 组件如何连接

```mermaid
---
config:
  flowchart:
    nodeSpacing: 28
    rankSpacing: 32
---
flowchart TB
    tui["独立 TUI"] --> client["Go SDK<br/>启动 · 观察 · 取消 · 取结果"]
    host["Go 宿主"] --> client
    client <--> mesh["Mesh · 能力寻址与调用传输<br/>本地 / 远程 · 请求 / 流"]

    subgraph execution["执行与观察"]
        direction TB
        runner["运行器 A<br/>推进循环与裁决终态"]
        events["运行事件 A<br/>流式进度与订阅"]
        runner ~~~ events
    end
    subgraph capabilities["能力与环境"]
        direction TB
        model["模型适配器 A<br/>统一模型输出流"]
        tools["工具提供方 A<br/>执行获授工具"]
        env["执行环境<br/>本机 / 沙箱 / 远程"]
        model ~~~ tools ~~~ env
    end
    subgraph state["状态与会话"]
        direction TB
        recovery["检查点与恢复 B<br/>保存状态与接续执行"]
        sessions["会话与调度 C<br/>历史与提交顺序"]
        recovery ~~~ sessions
    end
    mesh <--> execution
    mesh <--> capabilities
    mesh <--> state

    classDef entry fill:#f2f5f7,color:#20323d,stroke:#a4b3bd;
    classDef zone fill:#f6f8fa,color:#20323d,stroke:#c6d1da;
    classDef routing fill:#166071,color:#fff,stroke:#166071;
    classDef runtime fill:#e6f3f5,color:#142f37,stroke:#79a9b2;
    classDef placement fill:#ecf4e7,color:#263723,stroke:#86a57c;
    classDef later fill:#f0eef6,color:#332c44,stroke:#a298b7;
    class tui,host,client entry;
    class execution,capabilities,state zone;
    class mesh routing;
    class runner,model,tools,events runtime;
    class env placement;
    class recovery,sessions later;
```

每个组件按契约独立接入 Mesh。Mesh 负责寻址和传输，运行器负责单次 Run 的状态与终态。阶段 A 使用进程内绑定。 [Mesh 传输图解](mesh-transport.md)

## 谁负责什么

| 职责 | 归属 |
| --- | --- |
| Loop 内的观察 | 运行器收集模型回复和工具结果，据此决定下一步 |
| 宿主观察进度 | 运行器发布有序事件，事件组件管理订阅和背压，宿主决定何时开始或停止观察 |
| 单次 Run 的预算 | 运行器检查额度并决定何时停止。模型和执行环境分别执行各自的资源上限 |
| 工具权限 | 宿主授予可用能力。运行器在派发前检查，工具提供方复核业务权限 |
| 统一派发工具 | 运行器发起调用，Mesh 负责传输，工具提供方执行并回传结果 |
| AI API 工厂 | 模型侧装配客户端，模型适配器封装供应商请求和响应 |
| 读写会话历史 | 阶段 A 由宿主负责，阶段 C 由会话模块负责 |

## 消息由谁构造

```mermaid
flowchart TB
    input["宿主或 TUI<br/>构造本次用户消息"] --> runner["运行器<br/>组装每轮模型消息"]
    history["宿主 A / 会话模块 C<br/>读取历史快照"] --> runner
    runner -->|本轮 ModelRequest| model["模型适配器<br/>编码供应商请求"]
    runner -->|结束时交回 Result.Messages| commit["宿主 A / 会话模块 C<br/>按策略提交历史"]

    classDef owner fill:#166071,color:#fff,stroke:#166071;
    class runner owner;
```

每轮模型消息由历史快照、本次用户输入及当前 Run 已提交的轮次组成。工具调用和结果齐全后才一起加入上下文。 [消息组装图解](message-assembly.md)

## 工作在哪里执行

```mermaid
flowchart TB
    runner["运行器 · 本地或远程"] -->|"经 Mesh 派发"| tool["工具提供方 · 本地或远程"]
    tool -->|"经 Mesh 调用已授予的环境"| environment["执行环境契约"]
    environment --> local["本机环境<br/>使用宿主运行资源"]
    environment --> sandbox["本机隔离沙箱<br/>使用受限工作区与配额"]
    environment --> remote["远程工作节点<br/>可在节点上再启用沙箱"]

    classDef entry fill:#f2f5f7,color:#20323d,stroke:#a4b3bd;
    classDef placement fill:#ecf4e7,color:#263723,stroke:#86a57c;
    class runner,tool entry;
    class environment,local,sandbox,remote placement;
```

当前交付使用进程内运行器和本机只读环境。远程工具通过 mTLS 接入，并在提供方侧访问工作区。图中的沙箱、远程运行器和远程执行环境是后续绑定位置。工具按次领用环境权限。 [执行环境图解](execution-environment.md) · [工具调用与回执图解](tool-call-lifecycle.md)

## 阶段与取舍

| 阶段 | 边界 |
| --- | --- |
| A · 单次执行 | 先做 bypass。用本地 Run 验证模型流、工具、权限、预算和终态。 [最小闭环图解](stage-a-acceptance.md) |
| B · 恢复 | 定义检查点和中断恢复。外部操作结果不明时，先查回执或等待确认 |
| C · 会话与调度 | 管理历史读写、历史快照和提交顺序。建议同一会话先允许一个活跃 Run |

并发验收用两路独立对话。甲在等慢工具时，乙可以开始回复。取消甲后，乙继续输出。

[详细架构与 Mesh 契约](../specs/architecture.md) · [Go SDK 图解](go-sdk.md)
