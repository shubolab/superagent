# 阶段 A · 本地有界搜索闭环

## Why

纯文本 Run 已通过生命周期验收。第二步用一次真实的受限文件读取，验证模型提出工具、宿主授权、执行与回执回到模型的完整链路。

## What Changes

- 运行器从有效的 `ModelCompleted(tool_calls)` 进入串行工具批次，经本地 Mesh 申请按次授权、调用 `Tool.Invoke` 并结算预算。
- 增加只读的单文件字面搜索。执行环境按授权范围校验实际读取对象，搜索限制扫描、缓冲、命中和回执大小。
- 工具结果齐全后按 `CallID` 整批提交，再发起下一轮；验收已知拒绝、额度不足和取消时的行为。

## Capabilities

### New Capabilities

- `bounded-file-search` 定义本地按次文件授权、受限搜索和覆盖状态。

### Modified Capabilities

- `run-lifecycle` 扩展到授权工具批次、逐轮预算结算、结果回传及在途工具收尾。

## Impact

扩展现有 Go SDK、共享消息与事件类型、运行器和进程内 Mesh，并增加本地读取环境与搜索实现。后续切片负责丢失写入回执的完整故障验收、真实供应商和上下文压缩。

[阶段 A 实现顺序](../../../docs/specs/stage-a-acceptance.md#实现顺序)
