# 上下文什么时候压缩

阶段 A 基线由宿主在启动前选择完整历史轮次。以下是后续可选的 Run 内压缩设计。

```mermaid
---
config:
  flowchart:
    nodeSpacing: 28
    rankSpacing: 26
---
flowchart TB
    count["模型派发前 · 计量完整输入<br/>从窗口预留输出和余量"]
    count -->|低于门槛且未超限| send["按预算发送"]
    count -->|到达门槛或超限| keep["保留全部用户消息原文<br/>以及固定约束和近期轮次"]
    keep --> shrink["先用证据引用缩减旧正文<br/>空间仍不足时摘要旧轮次"]
    shrink --> check["校验来源、配对和剩余额度"]
    check -->|通过| send
    check -->|候选失败| fallback["依次检查缩减视图、<br/>补齐后的上版和完整原文"]

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class count,keep bound;
```

首轮试验在可用输入窗口的 **90%** 触发，目标压到 **70%**。窗口先扣掉实际输出预留和计量余量，还要满足 Run 的输入字节预算。阈值按模型和长任务验收校准。

例如，一个工具轮次里写了两份文件，工具声明和两个结果一起保留或一起归入摘要。可找回的产物用引用保留证据，未知的工具结果仍由 Run 的原规则收尾。宿主从 `Wait` 取得的仍是原始已提交消息。

适配器发送前核对计量指纹。源引用经授权可按页回查；连续压缩时，用户消息的原文和顺序都保留。回退到上版视图时，运行器先补齐新提交的消息，再复核来源和剩余额度。

[接口与验收用例](../specs/context-compaction-contract.md) · [触发参数、算法和调研依据](../specs/context-compaction.md) · [消息组装图解](message-assembly.md)
