# 阶段 A · 模型绑定工厂

## Why

宿主可以直接登记模型提供方。多个逻辑模型还需要共享供应商客户端，并统一处理凭据解析、装配回滚和请求失败。

## What Changes

- 按 ProviderRef 装配客户端，按 ModelRef 创建可登记模型。
- 解析凭据一次，装配失败时释放已打开客户端，支持并发和幂等关闭。
- 将供应商认证、模型不存在和流错误映射到可识别的 Run 停止原因。

## Capabilities

### New Capabilities

- `model-binding-factory` 定义客户端装配、凭据、错误及生命周期。

## Impact

新增 Go 宿主工厂入口、供应商扩展接口及结构化模型错误。原 ModelProvider 继续可直接登记。
