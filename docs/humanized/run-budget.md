# Run 预算怎么用

```mermaid
flowchart TB
    host["宿主 · 默认值与硬上限"] --> runner["运行器 · 按余额逐次授予"]
    runner --> model["模型适配器<br/>限帧、缓冲和本轮字节"]
    runner --> tool["工具提供方<br/>限扫描、输出和回执"]
    tool --> env["执行环境<br/>限内存、时限和存储"]
    model --> charge["运行器 · 按用量结算"]
    tool --> charge
    charge --> stop["下一步额度不足时收尾"]

    classDef bound fill:#166071,color:#fff,stroke:#166071;
    class runner,charge bound;
```

模型输出和工具回执共用字节余额，每步最多领到剩余量和单次上限中的较小值。时间、调用次数、工作区扫描和原始输出各有自己的上限。

## grep 扫大文件

| 场景 | 达到上限后的动作 |
| --- | --- |
| 大文件一直无命中，或读到一条巨长的行 | 工具在读取或缓冲上限处停下，报告已扫字节和覆盖状态 |
| 命中太多，或者 stdout / stderr 持续输出 | 执行侧停止继续收集，只返回限长摘要、已确认命中或产物引用 |
| 通用命令吃满内存 | 执行环境回收整棵进程树。首版优先提供按块读取的有界搜索 |

工具已确认的限额结果可以交给模型缩小范围。Run 的额度不足以继续时，`Wait` 返回 `BudgetExceeded`。已派发工具的效果仍无法确认时，终态是 `ToolOutcomeUnknown`。

[预算分配、收尾与验收](../specs/run-budget.md) · [工具生命周期](tool-call-lifecycle.md)
