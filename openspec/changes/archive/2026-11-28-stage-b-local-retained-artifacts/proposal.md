# 阶段 B · 本地持久产物绑定

## Why

B5 已用夹具验证产物引用能跨进程接续。宿主还需要一个可实际装配的绑定，让同一台机器上的新进程用原引用找回已确认产物。

## What Changes

- 宿主显式装配本地持久绑定。工具分块写入并持久封存，运行器确认原回执后发布，由调用方按原引用分页读取。
- 规定回执与产物之间的崩溃恢复、跨代次发布，以及数据丢失、损坏、过期时的处理。
- 用独立进程和真实本地存储完成 B5，记录实际绑定的验收结果。

## Capabilities

### New Capabilities

### Modified Capabilities

- `local-tool-artifacts` 增加同宿主的持久发布、有界读取与过期回收。
- `run-recovery` 增加按原回执恢复发布和已结算产物不可用时的续跑门禁。

## Impact

同宿主、本地 Mesh 和私有持久目录。通过新增绑定复用 `ArtifactRef`、`OpenToolArtifact` 和 `Run.ReadArtifact`。B5 的完成状态以实际绑定验收为准。
