# 上下文与消息组装

```mermaid
flowchart TB
    host["宿主传入<br/>历史快照 + 本次输入"] --> runner["运行器组装本轮消息<br/>历史 + 输入 + 已提交轮次"]
    runner --> model["模型适配器<br/>按供应商格式发送"]
    model -->|提出工具调用| batch["工具轮次<br/>助手消息 + 按序配对的结果"]
    model -->|最终答复| result["Wait · 返回可追加增量"]
    batch -->|结果齐全| commit["运行器提交整轮"]
    batch -->|中途停止或结果不明| stopped["当轮留作展示和诊断"]
    commit --> next["进入下一轮模型上下文"]
    commit -->|Run 结束时| result
    stopped --> result

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class runner,commit bound;
```

宿主单独传指令和本次输入。历史按完整轮次选取，运行器在受理时固定快照。模型每轮看到同一份历史、本次输入和本 Run 已提交的轮次。

| 场景 | 哪些消息可以保存 |
| --- | --- |
| 模型生成最终答复 | 本次输入、已提交工具轮次、最终助手消息 |
| 同轮两个工具，一个成功、一个明确拒绝 | 两个结果都齐了，整轮可一起保存 |
| 同轮一个工具完成，另一个效果不明 | 本次输入和此前已提交的轮次。当轮执行结局随事件和终态报告 |

宿主按终态决定是否把 `Result.Messages` 追加到会话历史。该增量已包含本次输入及可用的最终答复，重复读取 `Wait` 时按 `RunID` 去重。

[历史、组装和提交契约](../specs/message-assembly.md) · [Agent Loop](go-sdk.md#agent-loop) · [上下文压缩设计](context-compaction.md)
