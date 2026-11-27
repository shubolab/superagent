# 阶段 A · 本地纯文本 Run

## Why

先用可控模型验证 Go 宿主、进程内 Mesh 和运行器之间的最小闭环。生命周期和终态可独立验收后，再接工具循环。

## What Changes

- 定义 Go SDK、共享契约、Mesh 和运行器的包边界与最小接口。
- 通过编程式本地绑定运行一次纯文本模型请求，区分实时增量、完整回复和 Run 终态。
- 验收 `Start`、`Observe`、`Wait`、`Cancel`，覆盖模型授权、字节预算、提前断流和取消竞争。

## Capabilities

### New Capabilities

- `run-lifecycle` 定义本地 Run 的受理、观察、等待、取消和终态。

### Modified Capabilities

## Impact

后续实现会建立 Go module、公开 SDK 和进程内组件，并用可控模型经公开入口验收。工具执行、执行环境、真实供应商、文件配置和上下文压缩按各自后续切片接入。

[阶段 A 实现顺序](../../../../docs/specs/stage-a-acceptance.md#实现顺序)
